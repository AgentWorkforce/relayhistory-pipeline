/**
 * 20261105043700 owner buffered read RPCs. Seeds through the real buffered acceptance path
 * (POST /v1/turns/batches, frozen-supabase custody, lazy-current policy: real claim attempts,
 * stage intents, ACK decisions and native slots), then reads as bulls96_read_api with the
 * transaction-local bulls96_read.* binding the read API sets, passing the R2 control-head tuple
 * (contract v5.3: controlEpoch, head epoch/version/digest/kind/pending, privacyBaselineSha256) as
 * RPC parameters. Vectors follow the synthetic fixtures v3 + parity addendum v1:
 * cutover epoch 1x64, head digest 3x64, foreign epoch 2x64, target generation 4x64 (stale 8x64).
 * Admissions other than 0, control receipts and parity proofs are written directly as the owner
 * (the superuser here): no writer function exists for them.
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
import acl from "../../hoopdb/sessions-function-acl.json" with {type:"json"};
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
setDefaultTimeout(60000);

const root=join(import.meta.dir,"../../.."),generation="4".repeat(64),epoch="1".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
const E1=epoch,E2="2".repeat(64),D3="3".repeat(64),D2="2".repeat(64),G8="8".repeat(64),D9="9".repeat(64);
/** psql meta-commands (\set) are not SQL. */
const psql=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter((l)=>!l.startsWith("\\")).join("\n");
const MIGRATION="supabase/migrations/20261105043700_owner_buffered_read.sql";
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});

async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of SESSION_BOOTSTRAP_FILES)await db.exec(read(f));
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
 let who:Tenant=TENANT;
 const app=createIngestApp({identify:()=>({...who,scopes:["rth:sync"]}),deliver:deps,log:()=>{},custodySource:"frozen-supabase"});
 let serial=0;
 /** One accepted buffered turn batch for (source, session) as tenant t (default TENANT); returns its batch id. */
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
 /** Read as bulls96_read_api, bound like the read API binds a verified identity. */
 const asReader=async(sql:string,params:unknown[]=[],bind:{org?:string;ws?:string;user?:string|null}={},role="bulls96_read_api")=>{
  await db.exec("BEGIN READ ONLY");
  try{
   await db.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true),set_config('bulls96_read.user_id',$3,true)",[bind.org??TENANT.orgId,bind.ws??TENANT.workspaceId,bind.user===undefined?TENANT.userId:(bind.user??"")]);
   await db.exec("SET LOCAL ROLE "+role);
   const r=await db.query(sql,params as never);await db.exec("COMMIT");return r.rows as any[];
  }catch(e){await db.exec("ROLLBACK");throw e}
 };
 const hp=(h:Head)=>[h.cutover,h.epoch,h.version,h.digest,h.kind,h.pending,h.baseline];
 const list=(limit:number|null=null,cursor:[string|null,string|null,string|null]=[null,null,null],bind={},h:Head=NONE)=>asReader("SELECT source,session_id,first_ts,last_ts,last_ts::text AS cursor_ts,stage_count,bytes::text AS bytes,user_id,admission_epoch,admission_version FROM sessions.buffered_sessions_for($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",[TENANT.workspaceId,cursor[0],cursor[1],cursor[2],limit,...hp(h)],bind);
 const stages=(session:string,source="claude",bind={},h:Head=NONE)=>asReader("SELECT batch_id,stage_key,stage_sha256,stage_bytes::text AS stage_bytes,group_indexes,group_ordinals,kept_ordinals,reset_epoch::text AS reset_epoch,accepted_at,admission_epoch,admission_version FROM sessions.buffered_session_stages_for($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",[TENANT.workspaceId,source,session,...hp(h)],bind);
 /** Test-only raw owner G2 row write (delete + insert, so negative tests can plant any row; NOT the production
  *  writer contract in the migration header, which is CAS, never a blind upsert). Digests come from the canonical
  *  helper unless o overrides a field. */
 const prove=async(h:Head=NONE,o:{org?:string;ws?:string;user?:string;generation?:string;deletion?:string;reset?:string;lineage?:string;rowHeadDigest?:string;rowHeadKind?:string|null;baseline?:string}={})=>{
  const key=[o.org??TENANT.orgId,o.ws??TENANT.workspaceId,o.user??TENANT.userId];
  await db.exec("BEGIN");
  try{
   await db.query("DELETE FROM sessions.owner_buffered_parity WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3",key);
   await db.query(`INSERT INTO sessions.owner_buffered_parity(org_id,workspace_id,user_id,cutover_epoch,head_epoch,head_version,head_digest,head_kind,no_head,privacy_baseline_sha256,deletion_set_digest,reset_set_digest,target_lineage_digest,target_generation)
   SELECT $1,$2,$3,$4,$5::text,$6::bigint,$12::text,$14::text,$5::text IS NULL,$15::text,coalesce($8,e.deletion_set_digest),coalesce($9,e.reset_set_digest),coalesce($10,e.target_lineage_digest),coalesce($11,e.target_generation)
     FROM sessions.owner_buffered_parity_expected($1,$2,$3,$4,$6::bigint,$7,$13::text) AS e`,
    [...key,h.cutover,h.epoch,h.version,h.digest,o.deletion??null,o.reset??null,o.lineage??null,o.generation??null,o.rowHeadDigest??h.digest,h.kind,o.rowHeadKind!==undefined?o.rowHeadKind:h.kind,o.baseline??h.baseline??baseline]);
   await db.exec("COMMIT");
  }catch(e){await db.exec("ROLLBACK");throw e}
 };
 /** Control receipts from..to at epoch for the caller (owner insert); version `to` carries tip, others a synthetic digest. */
 const receipts=async(to:number,tip=D3,o:{from?:number;skip?:number[];epoch?:string;user?:string;kind?:string}={})=>{
  for(let v=o.from??1;v<=to;v++){if(o.skip?.includes(v))continue;
   await db.query("INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation) VALUES($1,$2,$3,$4,$5,$6,$8,'{}',$7)",[TENANT.orgId,TENANT.workspaceId,o.user??TENANT.userId,o.epoch??E1,v,v===to?tip:v.toString(16).padStart(64,"0"),generation,v===to?(o.kind??"selection"):"selection"]);}
 };
 /** Set the immutable admission version of every current claim (and its ACK decisions) of the caller. */
 const admit=async(version:string|null,where="TRUE")=>{
  await db.query("UPDATE sessions.ingest_claim_attempts SET source_control_version=$1::bigint WHERE user_id=$2 AND "+where,[version,TENANT.userId]);
  if(version!==null)await db.query("UPDATE sessions.ingest_lazy_ack_decisions d SET source_control_version=$1::bigint FROM sessions.ingest_claim_attempts a WHERE a.claim_token=d.claim_token AND a.user_id=$2 AND "+where.replace(/\bbatch_id\b/g,"a.batch_id"),[version,TENANT.userId]);
 };
 return {db,owner,upload,asReader,list,stages,prove,receipts,admit,ledger};
}
type Tenant={orgId:string;workspaceId:string;userId:string};
type Head={cutover:string|null;epoch:string|null;version:string|null;digest:string|null;kind:string|null;pending:boolean|null;baseline:string|null};
/** The R2 answer with no head for the caller at the active cutover epoch (the wire's version "0", null epoch/digest/kind). */
const NONE:Head={cutover:E1,epoch:null,version:null,digest:null,kind:null,pending:false,baseline:"a".repeat(64)};
/** A completed head (cutover epoch, version, digest, kind selection). */
const at=(version:number|string,digest=D3,o:Partial<Head>={}):Head=>({cutover:E1,epoch:E1,version:String(version),digest,kind:"selection",pending:false,baseline:"a".repeat(64),...o});
const S=(n:number)=>`7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a${String(n).padStart(2,"0")}`;

test("(a) the owner sees their buffered session with stage_count, bytes and acceptance times",async()=>{
 const f=await fixture();
 await f.upload(S(1));await f.upload(S(1),3,"claude",2);
 // Without the owner-written G2 proof nothing is visible.
 expect(await f.list()).toEqual([]);expect(await f.stages(S(1))).toEqual([]);
 await f.prove();
 // The real buffered path admits at version 0 with the policy's source identity.
 expect((await f.db.query("SELECT DISTINCT source_control_version::text v,source_identity_sha256=$1 AS same FROM sessions.ingest_claim_attempts",[identity])).rows).toEqual([{v:"0",same:true}]);
 const rows=await f.list();
 const ledger=(await f.db.query("SELECT sum(i.stage_bytes)::text AS bytes,min(b.committed_at) AS first,max(b.committed_at) AS last FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token JOIN sessions.ingest_batches b ON b.org_id=o.org_id AND b.workspace_id=o.workspace_id AND b.origin_id=o.origin_id AND b.batch_id=o.batch_id")).rows[0] as any;
 expect(rows.map(({cursor_ts,...r})=>r)).toEqual([{source:"claude",session_id:S(1),first_ts:ledger.first,last_ts:ledger.last,stage_count:2,bytes:ledger.bytes,user_id:TENANT.userId,admission_epoch:E1,admission_version:"0"}]);
 const st=await f.stages(S(1));
 expect(st.map((s)=>[s.batch_id,s.group_indexes,s.group_ordinals,s.kept_ordinals,s.admission_epoch,s.admission_version])).toEqual([["b-1",[0],[[0,1]],[0,1],E1,"0"],["b-2",[0],[[0,1,2]],[0,1,2],E1,"0"]]);
 expect(st.map((s)=>[s.stage_key.includes("/buffered-batches/v1/ws=ws-a/"),/^[0-9a-f]{64}$/.test(s.stage_sha256),s.reset_epoch])).toEqual([[true,true,"0"],[true,true,"0"]]);
 // A later revision of the same turn supersedes the older one: the GC would drop it, so does the read.
 await f.upload(S(1),1,"claude",0);
 // group_ordinals stays the stage's whole stored list; kept_ordinals narrows.
 expect((await f.stages(S(1))).map((s)=>[s.batch_id,s.group_ordinals,s.kept_ordinals])).toEqual([["b-1",[[0,1]],[1]],["b-2",[[0,1,2]],[0,1,2]],["b-3",[[0]],[0]]]);
});

/** Fenced authority and malformed head shapes are the same 55006 'buffered read fenced'. */
async function fenced(p:Promise<unknown>){
 const e=await p.then(()=>null,(e)=>e) as any;
 expect([e?.code,e?.message]).toEqual(["55006","buffered read fenced"]);
}
async function code(p:Promise<unknown>){const e=await p.then(()=>null,(e)=>e) as any;return e?.code as string|undefined}
const ids=(rows:any[])=>rows.map((r)=>r.session_id);

test("(b) cross-user: another user's binding sees neither the list nor the stages",async()=>{
 const f=await fixture();await f.upload(S(1));await f.prove();await f.prove(NONE,{user:"user-2"});
 expect(await f.list(null,undefined,{user:"user-2"})).toEqual([]);
 expect(await f.stages(S(1),"claude",{user:"user-2"})).toEqual([]);
 expect(ids(await f.list())).toEqual([S(1)]);
});

test("(c) a deleted session is absent from list and stages; the stale proof hides everything until rewritten",async()=>{
 const f=await fixture();await f.upload(S(1));await f.upload(S(2));await f.prove();
 await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude',$5)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,TURN_ORIGIN_ID,S(1)]);
 // full-deletion-set-mismatch: the proof's deletion_set_digest no longer matches.
 expect(await f.list()).toEqual([]);expect(await f.stages(S(2))).toEqual([]);
 await f.prove();
 expect(ids(await f.list())).toEqual([S(2)]);
 expect(await f.stages(S(1))).toEqual([]);
});

test("(d) a reset since staging hides every session of the user, even under a rewritten proof",async()=>{
 const f=await fixture();await f.upload(S(1));await f.prove();
 const before=(await f.db.query("SELECT reset_set_digest r FROM sessions.owner_buffered_parity")).rows[0] as any;
 await f.db.query("INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id) VALUES($1,$2,$3,'r-1')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
 expect(await f.list()).toEqual([]);
 await f.prove();
 expect(((await f.db.query("SELECT reset_set_digest r FROM sessions.owner_buffered_parity")).rows[0] as any).r).not.toBe(before.r);
 expect(await f.list()).toEqual([]);
 expect(await f.stages(S(1))).toEqual([]);
});

test("(e) a fenced attempt (fence version moved on) is absent",async()=>{
 const f=await fixture();await f.upload(S(1));await f.prove();await f.upload(S(2));
 await f.db.query("INSERT INTO sessions.ingest_fence_versions(org_id,workspace_id,user_id,origin_id,source,session_id,epoch) VALUES($1,$2,$3,$4,'claude',$5,99) ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=sessions.ingest_fence_versions.epoch+99",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,TURN_ORIGIN_ID,S(1)]);
 expect(ids(await f.list())).toEqual([S(2)]);
 expect(await f.stages(S(1))).toEqual([]);
});

test("(f) a redacted stage intent is absent",async()=>{
 const f=await fixture();await f.upload(S(1));await f.prove();await f.upload(S(2));
 await f.db.query("UPDATE sessions.ingest_stage_intents SET cleanup_state='redacted' WHERE batch_id='b-1'");
 expect(ids(await f.list())).toEqual([S(2)]);
 expect(await f.stages(S(1))).toEqual([]);
});

test("(g) a fenced privacy authority refuses with 'buffered read fenced'",async()=>{
 const f=await fixture();await f.upload(S(1));await f.prove();
 await f.db.query("INSERT INTO sessions.ingest_privacy_authority(singleton,state,control_epoch) VALUES(true,'fenced',$1)",[epoch]);
 await fenced(f.list());await fenced(f.stages(S(1)));
});

test("(h) any role other than bulls96_read_api is refused 42501, and the scope is owner-only",async()=>{
 const f=await fixture();await f.upload(S(1));
 for(const role of ["bulls96_session_ingest","anon","authenticated","service_role"])
  expect(await code(f.asReader("SELECT * FROM sessions.buffered_sessions_for($1,null,null,null,10,$2,null,null,null,null,false,repeat('a',64))",[TENANT.workspaceId,E1],{},role))).toBe("42501");
 for(const sql of ["SELECT * FROM sessions.owner_buffered_read_scope($1,$2,null,null,null,null,false,repeat('a',64))","SELECT * FROM sessions.owner_buffered_parity_expected('o',$1,'u',$2,null,null,null)"])
  expect(await code(f.asReader(sql,[TENANT.workspaceId,E1]))).toBe("42501");
 // The parity table: no privilege for any app role.
 for(const role of ["bulls96_read_api","bulls96_session_ingest","bulls96_session_ingest_seeder","anon","authenticated","service_role"])
  expect(await code(f.asReader("SELECT count(*) FROM sessions.owner_buffered_parity",[],{},role))).toBe("42501");
});

test("(i) a workspace mismatch or a missing user binding is refused 42501",async()=>{
 const f=await fixture();await f.upload(S(1));await f.prove();
 expect(await code(f.list(null,undefined,{ws:"ws-b"}))).toBe("42501");
 expect(await code(f.list(null,undefined,{user:null}))).toBe("42501");
 expect(await code(f.stages(S(1),"claude",{org:""}))).toBe("42501");
});

test("(j) keyset cursor: limit 2 over 5 sessions returns all 5 exactly once, newest first",async()=>{
 const f=await fixture();await f.prove();
 for(let n=1;n<=5;n++)await f.upload(S(n));
 const seen:string[]=[];let cursor:[string|null,string|null,string|null]=[null,null,null];
 for(let page=0;page<5;page++){
  const rows=await f.list(2,cursor);if(!rows.length)break;
  expect(rows.length).toBeLessThanOrEqual(2);seen.push(...ids(rows));
  const last=rows.at(-1);cursor=[last.cursor_ts,last.session_id,last.source];
 }
 expect(seen).toEqual([S(5),S(4),S(3),S(2),S(1)]);
 expect(await code(f.list(2,["2026-11-05 00:00:00+00",null,null]))).toBe("22023");
});

test("(k) the limit is clamped to 1..100 and NULL means 50",async()=>{
 const f=await fixture();await f.prove();
 // A turn batch holds one session: 101 one-turn uploads.
 for(let n=0;n<101;n++)await f.upload(S(n),1);
 expect((await f.list(0)).length).toBe(1);
 expect((await f.list(-5)).length).toBe(1);
 expect((await f.list(null)).length).toBe(50);
 expect((await f.list(1000)).length).toBe(100);
});

test("(l) no row or advisory locks: pg_locks inside the read transaction, and a static check",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);await f.prove(at(7));
 await f.db.exec("BEGIN READ ONLY");
 await f.db.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true),set_config('bulls96_read.user_id',$3,true)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
 await f.db.exec("SET LOCAL ROLE bulls96_read_api");
 const listed=(await f.db.query("SELECT * FROM sessions.buffered_sessions_for($1,null,null,null,10,$2,$2,'7',$3,'selection',false,$4)",[TENANT.workspaceId,E1,D3,baseline])).rows;
 const staged=(await f.db.query("SELECT * FROM sessions.buffered_session_stages_for($1,'claude',$2,$3,$3,'7',$4,'selection',false,$5)",[TENANT.workspaceId,S(1),E1,D3,baseline])).rows;
 const locks=(await f.db.query("SELECT locktype,mode FROM pg_locks WHERE pid=pg_backend_pid()")).rows as any[];
 await f.db.exec("COMMIT");
 // The gate was open, so the keep query really ran.
 expect([listed.length,staged.length]).toEqual([1,1]);
 expect(locks.length).toBeGreaterThan(0);
 expect(locks.filter((l)=>!(l.locktype==="virtualxid"||(l.locktype==="relation"&&l.mode==="AccessShareLock")))).toEqual([]);
 const sql=readFileSync(join(root,MIGRATION),"utf8");
 const code=sql.split("\n").filter((l)=>!l.trim().startsWith("--")).join("\n");
 expect(code).not.toMatch(/\bFOR\s+(UPDATE|SHARE|NO\s+KEY\s+UPDATE|KEY\s+SHARE)\b|pg_(try_)?advisory|\bLOCK\s+TABLE\b|\b(INSERT|UPDATE|DELETE)\s/i);
 const blocks=[...sql.matchAll(/-- keep-predicate:begin[\s\S]*?-- keep-predicate:end/g)].map((m)=>m[0]);
 expect(blocks.length).toBe(2);expect(blocks[0]).toBe(blocks[1]);
});

// ---- R2 control-head contract v5.3: the gate, the per-row admission witness and the G2 parity proof ----
const visible=async(f:Awaited<ReturnType<typeof fixture>>,h:Head,session=S(1))=>[ids(await f.list(null,undefined,{},h)),(await f.stages(session,"claude",{},h)).length];
const HIDDEN=[[],0];

test("(m) healthy head: admission (E1,7), contiguous receipts 1..7, proof at 7 shows list and stages with the admission echo",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);
 expect(await visible(f,at(7))).toEqual(HIDDEN);
 await f.prove(at(7));
 const rows=await f.list(null,undefined,{},at(7));
 expect(rows.map((r)=>[r.session_id,r.admission_epoch,r.admission_version])).toEqual([[S(1),E1,"7"]]);
 const st=await f.stages(S(1),"claude",{},at(7));
 expect(st.map((s)=>[s.batch_id,s.admission_epoch,s.admission_version])).toEqual([["b-1",E1,"7"]]);
 // The no-head answer does not open a head-7 proof.
 expect(await visible(f,NONE)).toEqual(HIDDEN);
});

test("(n) ABA: an admission at 5 under head 7 stays hidden; re-admitted at 7 it is shown",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("5");await f.receipts(7);await f.prove(at(7));
 expect(await visible(f,at(7))).toEqual(HIDDEN);
 await f.admit("7");
 expect(await visible(f,at(7))).toEqual([[S(1)],1]);
 // An ACK decision of another version under the same claim drops the ordinal.
 await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET source_control_version=5");
 expect(await visible(f,at(7))).toEqual(HIDDEN);
});

test("(o) same version, different epoch: cutover E2 or head epoch E2 is hidden",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);await f.prove(at(7));
 expect(await visible(f,at(7))).toEqual([[S(1)],1]);
 expect(await visible(f,at(7,D3,{epoch:E2}))).toEqual(HIDDEN);
 expect(await visible(f,at(7,D3,{cutover:E2,epoch:E2}))).toEqual(HIDDEN);
 // Even a proof written for the E2 cutover: the policy epoch is E1.
 await f.receipts(7,D3,{epoch:E2});
 expect(await visible(f,at(7))).toEqual(HIDDEN);
});

test("(p) missing admission version, missing or mismatched epoch identity, policy expired or deleted: hidden",async()=>{
 const f=await fixture();await f.upload(S(1));await f.prove();
 expect(await visible(f,NONE)).toEqual([[S(1)],1]);
 await f.admit(null);expect(await visible(f,NONE)).toEqual(HIDDEN);
 await f.admit("0");expect(await visible(f,NONE)).toEqual([[S(1)],1]);
 const idn=async(v:string|null)=>f.db.query("UPDATE sessions.ingest_claim_attempts SET source_identity_sha256=$1",[v]);
 await idn("f".repeat(64));expect(await visible(f,NONE)).toEqual(HIDDEN);
 await idn(null);expect(await visible(f,NONE)).toEqual(HIDDEN);
 await idn(identity);expect(await visible(f,NONE)).toEqual([[S(1)],1]);
 await f.db.query("UPDATE sessions.ingest_lazy_policy SET valid_until=clock_timestamp()-interval '1 second'");
 expect(await visible(f,NONE)).toEqual(HIDDEN);
 await f.db.query("DELETE FROM sessions.ingest_lazy_policy");
 expect(await visible(f,NONE)).toEqual(HIDDEN);
});

test("(q) head NULL with a nonzero admission is hidden; a NULL cutover is fenced",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.prove();
 expect(await visible(f,NONE)).toEqual(HIDDEN);
 await fenced(f.list(null,undefined,{},{...NONE,cutover:null}));
 await fenced(f.stages(S(1),"claude",{},{...NONE,cutover:null}));
});

test("(r) head NULL with admission (cutover,0): proven is visible; unproven hidden; receipts under a no-head proof hide",async()=>{
 const f=await fixture();await f.upload(S(1));
 expect(await visible(f,NONE)).toEqual(HIDDEN);
 await f.prove();
 expect(await visible(f,NONE)).toEqual([[S(1)],1]);
 const rows=await f.list(null,undefined,{},NONE);expect([rows[0].admission_epoch,rows[0].admission_version]).toEqual([E1,"0"]);
 await f.receipts(1);
 expect(await visible(f,NONE)).toEqual(HIDDEN);
 await f.prove();expect(await visible(f,NONE)).toEqual(HIDDEN);
});

test("(s) any later control (receipt 8) hides the head-7 view and the head-8 view of a 7-admission",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);await f.prove(at(7));
 expect(await visible(f,at(7))).toEqual([[S(1)],1]);
 await f.receipts(8,D2,{from:8});
 expect(await visible(f,at(7))).toEqual(HIDDEN);
 await f.prove(at(8,D2));
 expect(await visible(f,at(8,D2))).toEqual(HIDDEN);
 await f.prove(at(7));expect(await visible(f,at(7))).toEqual(HIDDEN);
});

test("(t) a GC replacement stage binds the ORIGINAL claim; pointing it at a newer claim hides it",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);await f.prove(at(7));
 // GC replacement: a child stage (new token and key, parent = original), carrying the original claim.
 const child=(await f.db.query(`INSERT INTO sessions.ingest_stage_intents
   SELECT (jsonb_populate_record(NULL::sessions.ingest_stage_intents,to_jsonb(s)||jsonb_build_object('stage_token',gen_random_uuid(),'stage_key',s.stage_key||'-r','parent_stage_token',s.stage_token))).*
     FROM sessions.ingest_stage_intents s WHERE s.batch_id='b-1' RETURNING stage_token::text t`)).rows[0] as any;
 await f.db.query("UPDATE sessions.ingest_buffer_outbox SET active_stage_token=$1::uuid WHERE batch_id='b-1'",[child.t]);
 expect(await visible(f,at(7))).toEqual([[S(1)],1]);
 expect((await f.stages(S(1),"claude",{},at(7)))[0].stage_key).toMatch(/-r$/);
 // A copied claim (same batch identity) and its ACK decisions.
 const copy=async(version:string)=>{
  const c=(await f.db.query(`INSERT INTO sessions.ingest_claim_attempts
    SELECT (jsonb_populate_record(NULL::sessions.ingest_claim_attempts,to_jsonb(a)||jsonb_build_object('claim_token',gen_random_uuid(),'source_control_version',$1::bigint))).*
      FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_stage_intents s ON s.claim_token=a.claim_token AND s.stage_token=$2::uuid RETURNING claim_token::text t`,[version,child.t])).rows[0] as any;
  await f.db.query(`INSERT INTO sessions.ingest_lazy_ack_decisions
    SELECT (jsonb_populate_record(NULL::sessions.ingest_lazy_ack_decisions,to_jsonb(d)||jsonb_build_object('claim_token',$1::uuid,'source_control_version',$2::bigint))).*
      FROM sessions.ingest_lazy_ack_decisions d JOIN sessions.ingest_stage_intents s ON s.claim_token=d.claim_token AND s.stage_token=$3::uuid`,[c.t,version,child.t]);
  return c.t as string;
 };
 // Same admission version, newer claim: the ancestor carries a different claim, so hidden.
 const same=await copy("7");
 await f.db.query("UPDATE sessions.ingest_stage_intents SET claim_token=$1::uuid WHERE stage_token=$2::uuid",[same,child.t]);
 expect(await visible(f,at(7))).toEqual(HIDDEN);
 // Version 8 claim behind a fully proven head 8: still hidden (the replacement cannot lend it).
 const newer=await copy("8");
 await f.db.query("UPDATE sessions.ingest_stage_intents SET claim_token=$1::uuid WHERE stage_token=$2::uuid",[newer,child.t]);
 await f.receipts(8,D2,{from:8});await f.prove(at(8,D2));
 expect(await visible(f,at(8,D2))).toEqual(HIDDEN);
});

test("(u) decimal versions are carried exactly as text through int8 (no float rounding)",async()=>{
 const f=await fixture();
 await f.db.exec("BEGIN");
 try{
  // The scope is not SECURITY DEFINER: a transaction-local grant of it and of the tables it reads.
  await f.db.exec("GRANT EXECUTE ON FUNCTION sessions.owner_buffered_read_scope(text,text,text,text,text,text,boolean,text) TO bulls96_read_api;GRANT SELECT ON sessions.ingest_privacy_authority,sessions.ingest_lazy_policy,sessions.owner_buffered_parity TO bulls96_read_api");
  await f.db.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true),set_config('bulls96_read.user_id',$3,true)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
  await f.db.exec("SET LOCAL ROLE bulls96_read_api");
  const echo=async(v:string)=>((await f.db.query("SELECT admission_version::text v,gate_open g FROM sessions.owner_buffered_read_scope($1,$2,$2,$3,$4,'selection',false,$5)",[TENANT.workspaceId,E1,v,D3,baseline])).rows[0] as any);
  expect(await echo("9007199254740993")).toEqual({v:"9007199254740993",g:false});
  expect(await echo("9007199254740992")).toEqual({v:"9007199254740992",g:false});
  expect(await echo("9223372036854775807")).toEqual({v:"9223372036854775807",g:false});
 }finally{await f.db.exec("ROLLBACK")}
 // Rolled back: the scope is owner-only again.
 expect(await code(f.asReader("SELECT * FROM sessions.owner_buffered_read_scope($1,$2,null,null,null,null,false,repeat('a',64))",[TENANT.workspaceId,E1]))).toBe("42501");
});

test("(v) G2 parity: missing, foreign, stale or mismatched proofs hide",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);
 const H=at(7),ok=[[S(1)],1];
 expect(await visible(f,H)).toEqual(HIDDEN);// missing row
 const reprove=async(o={})=>{await f.db.query("DELETE FROM sessions.owner_buffered_parity");await f.prove(H,o)};
 for(const o of [{user:"user-2"},{ws:"ws-b"},{org:"org-b"}]){await reprove(o);expect(await visible(f,H)).toEqual(HIDDEN)}
 await reprove();expect(await visible(f,H)).toEqual(ok);
 // A proof for another cutover epoch.
 await f.db.query("UPDATE sessions.owner_buffered_parity SET cutover_epoch=$1,head_epoch=$1",[E2]);expect(await visible(f,H)).toEqual(HIDDEN);
 await reprove({generation:G8});expect(await visible(f,H)).toEqual(HIDDEN);
 await reprove();await f.db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1",[G8]);
 expect(await visible(f,H)).toEqual(HIDDEN);
 await f.db.query("UPDATE sessions.__96bulls_role_validation SET status='fail',generation=$1",[generation]);
 await reprove().catch(()=>{});expect(await visible(f,H)).toEqual(HIDDEN);// no pass record: no generation
 await f.db.query("UPDATE sessions.__96bulls_role_validation SET status='pass'");
 await reprove({rowHeadDigest:D9});expect(await visible(f,H)).toEqual(HIDDEN);// wrong head digest
 for(const k of ["deletion","reset","lineage"]){await reprove({[k]:D9});expect(await visible(f,H)).toEqual(HIDDEN)}
 await reprove();expect(await visible(f,H)).toEqual(ok);
 // Stale head after a new admission: the proof at 7 does not open head 8.
 await f.receipts(8,D2,{from:8});await f.admit("8");
 expect(await visible(f,at(8,D2))).toEqual(HIDDEN);
 await f.prove(at(8,D2));expect(await visible(f,at(8,D2))).toEqual(ok);
 // full-deletion-set-mismatch: deleting an unrelated session changes the set and hides everything.
 await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude',$5)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,TURN_ORIGIN_ID,S(9)]);
 expect(await visible(f,at(8,D2))).toEqual(HIDDEN);
 await f.prove(at(8,D2));expect(await visible(f,at(8,D2))).toEqual(ok);
});

test("(x) a pending head hides; pending with no head, or pending NULL, is fenced",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);await f.prove(at(7));
 expect(await visible(f,at(7,D3,{pending:true}))).toEqual(HIDDEN);
 await fenced(f.list(null,undefined,{},{...NONE,pending:true}));
 await fenced(f.list(null,undefined,{},at(7,D3,{pending:null})));
 // Read/erase race: the head turns pending between the list and the detail call.
 expect(ids(await f.list(null,undefined,{},at(7)))).toEqual([S(1)]);
 expect(await f.stages(S(1),"claude",{},at(7,D3,{pending:true}))).toEqual([]);
});

test("(y) receipt gap, receipts behind the head, tip digest mismatch, or a receipt on another epoch: hidden",async()=>{
 const cases:Array<[string,(f:Awaited<ReturnType<typeof fixture>>)=>Promise<unknown>,Head]>=[
  ["gap",(f)=>f.receipts(7,D3,{skip:[3]}),at(7)],
  ["behind",(f)=>f.receipts(7),at(8)],
  ["tip",(f)=>f.receipts(7,D2),at(7)],
  ["other-epoch",async(f)=>{await f.receipts(7);await f.receipts(1,D3,{epoch:E2})},at(7)],
 ];
 for(const [name,setup,h] of cases){
  const f=await fixture();await f.upload(S(1));await f.admit(h.version);await setup(f);await f.prove(h);
  expect([name,await visible(f,h)]).toEqual([name,HIDDEN]);
 }
});

test("(z) malformed head shapes are fenced 55006 on both readers",async()=>{
 const f=await fixture();await f.upload(S(1));await f.prove();
 const bad:Head[]=[at("0"),at("-1"),at("07"),at("1.0"),at(" 7"),at("9223372036854775808"),at("99999999999999999999"),
  at(7,"A".repeat(64)),at(7,D3,{epoch:"A".repeat(64)}),at(7,D3,{cutover:"1".repeat(63)}),
  {...at(7),digest:null},{...at(7),epoch:null},{...NONE,version:"7"},{...NONE,cutover:"zz"}];
 for(const h of bad){await fenced(f.list(null,undefined,{},h));await fenced(f.stages(S(1),"claude",{},h))}
 // Well-formed but not proven is an empty answer, not an error.
 expect(await visible(f,at(7))).toEqual(HIDDEN);
});

test("(w) a session with a canonical publication head is excluded from the list; canonical wins",async()=>{
 const f=await fixture();await f.upload(S(1));await f.upload(S(2));await f.upload(S(3));await f.prove();
 const pub=(session:string,state:string,key:string|null)=>f.db.query("INSERT INTO sessions.ingest_publication_heads(org_id,workspace_id,user_id,origin_id,source,session_id,physical_workspace,physical_source,physical_session,machine_id,generation,fence_epoch,reset_epoch,pointer_revision,current_key,state) VALUES($1,$2,$3,$4,'claude',$5,$2,'claude',$5,'m-1',gen_random_uuid(),1,0,1,$6,$7)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,TURN_ORIGIN_ID,session,key,state]);
 await pub(S(1),"live","k/1");await pub(S(2),"building","k/2");await pub(S(3),"retired",null);
 expect(ids(await f.list())).toEqual([S(3)]);
 // The detail is not filtered: the read API serves canonical first and falls back only when canonical is empty.
 expect((await f.stages(S(1))).length).toBe(1);
});

// ---- contract v5.3 rule 4: privacy baseline and head kind are bound to the policy, the receipts and the parity row ----
test("(ba) a privacy baseline other than the policy's, or other than the parity row's, hides",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);await f.prove(at(7));
 const B="b".repeat(64),ok=[[S(1)],1];
 expect(await visible(f,at(7))).toEqual(ok);
 // Wire baseline B, policy and row A.
 expect(await visible(f,at(7,D3,{baseline:B}))).toEqual(HIDDEN);
 // Row and wire B, policy A: the immutable policy baseline still disagrees.
 await f.prove(at(7,D3,{baseline:B}));
 expect(await visible(f,at(7,D3,{baseline:B}))).toEqual(HIDDEN);
 // Wire and policy A, row B.
 expect(await visible(f,at(7))).toEqual(HIDDEN);
 await f.prove(at(7));expect(await visible(f,at(7))).toEqual(ok);
 // The no-head view is bound the same way.
 const g=await fixture();await g.upload(S(1));await g.prove(NONE,{baseline:B});
 expect(await visible(g,NONE)).toEqual(HIDDEN);
 await g.prove();expect(await visible(g,NONE)).toEqual(ok);
 expect(await visible(g,{...NONE,baseline:B})).toEqual(HIDDEN);
});

test("(bb) a missing or malformed privacy baseline is fenced 55006 on both readers, with or without a head",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);await f.prove(at(7));
 for(const b of [null,"A".repeat(64),"a".repeat(63),"a".repeat(65),"g".repeat(64),""," "+"a".repeat(63)])
  for(const h of [at(7,D3,{baseline:b}),{...NONE,baseline:b}]){await fenced(f.list(null,undefined,{},h));await fenced(f.stages(S(1),"claude",{},h))}
 expect(await visible(f,at(7))).toEqual([[S(1)],1]);
});

test("(bc) head kind: wire vs latest receipt kind, and wire vs parity row kind; mismatches hide, bad kinds fence",async()=>{
 const f=await fixture();await f.upload(S(1));await f.admit("7");await f.receipts(7);await f.prove(at(7));
 const ok=[[S(1)],1];
 expect(await visible(f,at(7))).toEqual(ok);
 // Wire 'reset' but receipt 7 is a selection (row also says reset): hidden.
 await f.prove(at(7,D3,{kind:"reset"}));
 expect(await visible(f,at(7,D3,{kind:"reset"}))).toEqual(HIDDEN);
 // Row kind 'reset', wire and receipt 'selection': hidden.
 await f.prove(at(7),{rowHeadKind:"reset"});
 expect(await visible(f,at(7))).toEqual(HIDDEN);
 await f.prove(at(7));expect(await visible(f,at(7))).toEqual(ok);
 // A reset head: receipt 8 of kind reset, re-admitted at 8, proven as reset.
 await f.receipts(8,D2,{from:8,kind:"reset"});await f.admit("8");await f.prove(at(8,D2,{kind:"reset"}));
 expect(await visible(f,at(8,D2,{kind:"reset"}))).toEqual(ok);
 expect(await visible(f,at(8,D2))).toEqual(HIDDEN);
 // Kind shapes: NULL with a head, unknown or wrong case, or a kind with no head.
 for(const h of [at(8,D2,{kind:null}),at(8,D2,{kind:"Reset"}),at(8,D2,{kind:"erase"}),at(8,D2,{kind:""}),{...NONE,kind:"selection"},{...NONE,kind:"reset"}])
  {await fenced(f.list(null,undefined,{},h));await fenced(f.stages(S(1),"claude",{},h))}
 // The parity row refuses a kind with no head, and a head with no kind.
 expect(await code(f.prove(NONE,{rowHeadKind:"selection"}))).toBe("23514");
 expect(await code(f.prove(at(8,D2),{rowHeadKind:null}))).toBe("23514");
});

test("(bd) no head: the only visible form is NULL epoch/version/digest/kind with pending false, under the new parameters",async()=>{
 const f=await fixture();await f.upload(S(1));await f.prove();
 expect(await visible(f,NONE)).toEqual([[S(1)],1]);
 const rows=await f.list(null,undefined,{},NONE);
 expect([rows[0].admission_epoch,rows[0].admission_version]).toEqual([E1,"0"]);
 const st=await f.stages(S(1),"claude",{},NONE);
 expect(st.map((s)=>[s.group_ordinals,s.kept_ordinals,s.admission_version])).toEqual([[[[0,1]],[0,1],"0"]]);
 for(const h of [{...NONE,pending:true},{...NONE,pending:null},{...NONE,kind:"selection"},{...NONE,version:"0"},{...NONE,digest:D3},{...NONE,epoch:E1}])
  await fenced(f.list(null,undefined,{},h));
});

test("(be) data of a foreign org, and of a foreign workspace with the same user id, is absent",async()=>{
 const f=await fixture();
 await f.upload(S(1));
 await f.upload(S(2),2,"claude",0,{...TENANT,orgId:"org-b"});
 await f.upload(S(3),2,"claude",0,{...TENANT,workspaceId:"ws-b"});
 expect((await f.db.query("SELECT org_id,workspace_id,user_id FROM sessions.ingest_buffer_outbox ORDER BY 1,2")).rows).toEqual([
  {org_id:"org-a",workspace_id:"ws-a",user_id:TENANT.userId},{org_id:"org-a",workspace_id:"ws-b",user_id:TENANT.userId},{org_id:"org-b",workspace_id:"ws-a",user_id:TENANT.userId}]);
 await f.prove();
 expect(ids(await f.list())).toEqual([S(1)]);
 expect(await f.stages(S(2))).toEqual([]);expect(await f.stages(S(3))).toEqual([]);
 // Even proofs for the foreign scopes do not leak them into the TENANT binding.
 await f.prove(NONE,{org:"org-b"});await f.prove(NONE,{ws:"ws-b"});
 expect(ids(await f.list())).toEqual([S(1)]);
 expect(await f.stages(S(2))).toEqual([]);expect(await f.stages(S(3))).toEqual([]);
 // Positive control: each scope sees exactly its own session under its own binding.
 expect(ids(await f.list(null,undefined,{org:"org-b"}))).toEqual([S(2)]);
 expect((await f.stages(S(2),"claude",{org:"org-b"})).length).toBe(1);
 expect((await f.stages(S(1),"claude",{org:"org-b"})).length).toBe(0);
});

// ---- contract v5.3 rule 5: defense in depth inside the keep predicate ----
test("(bf) rule 5(a): a frozen source deletion (any origin) hides the session without a new proof",async()=>{
 const f=await fixture();await f.upload(S(1));await f.upload(S(2));await f.prove();
 expect(ids(await f.list())).toEqual([S(2),S(1)]);
 await f.db.query("INSERT INTO sessions.ingest_frozen_source_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'some-other-origin','claude',$4)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,S(1)]);
 // Not part of the parity digest: the proof still matches, the predicate hides.
 expect(ids(await f.list())).toEqual([S(2)]);
 expect(await f.stages(S(1))).toEqual([]);
 expect((await f.stages(S(2))).length).toBe(1);
 // Another user's frozen deletion of the same session id does not hide it.
 const g=await fixture();await g.upload(S(1));await g.prove();
 await g.db.query("INSERT INTO sessions.ingest_frozen_source_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,'user-2',$3,'claude',$4)",[TENANT.orgId,TENANT.workspaceId,TURN_ORIGIN_ID,S(1)]);
 expect(ids(await g.list())).toEqual([S(1)]);
});

test("(bg) rule 5(b): an upload_deletions entry under ANY origin hides the session, even under a rewritten proof",async()=>{
 const f=await fixture();await f.upload(S(1));await f.upload(S(2));await f.prove();
 await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'some-other-origin','claude',$4)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,S(1)]);
 await f.prove();
 expect(ids(await f.list())).toEqual([S(2)]);
 expect(await f.stages(S(1))).toEqual([]);
 // A different source with the same session id is not covered.
 await f.db.query("DELETE FROM sessions.upload_deletions");
 await f.db.query("INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,'some-other-origin','codex',$4)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,S(1)]);
 await f.prove();
 expect(ids(await f.list())).toEqual([S(2),S(1)]);
});

test("(bh) rule 5(c): acceptance at or before an upload_resets entry hides, even under a fresh proof; a NULL admission reset epoch hides",async()=>{
 const f=await fixture();
 // A first upload binds the org admission (a first admission refuses prior org rows); the reset then retires it.
 await f.upload(S(9));
 await f.db.query("INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id,reset_at) VALUES($1,$2,$3,'r-1',now()-interval '1 hour')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
 await f.upload(S(1));await f.upload(S(2));await f.prove();
 expect((await f.stages(S(1))).map((s)=>s.reset_epoch)).toEqual(["1"]);
 expect(ids(await f.list())).toEqual([S(2),S(1)]);
 const acc=((await f.db.query("SELECT committed_at::text t FROM sessions.ingest_batches WHERE batch_id='b-2'")).rows[0] as any).t;
 // The reset set is part of the proof, so each change is re-proven: the hide below is the predicate's, not a stale proof.
 const resetAt=async(sql:string)=>{await f.db.query("UPDATE sessions.upload_resets SET reset_at="+sql+" WHERE request_id='r-1'",[acc]);await f.prove()};
 await resetAt("$1::timestamptz");
 expect(ids(await f.list())).toEqual([S(2)]);expect(await f.stages(S(1))).toEqual([]);
 await resetAt("$1::timestamptz+interval '1 hour'");
 expect(await f.list()).toEqual([]);
 // reset_at is NOT NULL; the predicate's NULL arm is fail-closed defense only.
 expect(await code(f.db.query("UPDATE sessions.upload_resets SET reset_at=NULL"))).toBe("23502");
 await resetAt("$1::timestamptz-interval '1 microsecond'");
 expect(ids(await f.list())).toEqual([S(2),S(1)]);
 // A NULL admission reset epoch on the claim (the stage's is NOT NULL) fails closed.
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET reset_epoch=NULL WHERE batch_id='b-2'");
 expect(ids(await f.list())).toEqual([S(2)]);expect(await f.stages(S(1))).toEqual([]);
});

test("the function ACLs equal the manifest and the read-role verifier passes",async()=>{
 const f=await fixture();
 const rows=(await f.db.query("SELECT p.oid::regprocedure::text AS fn,array_agg(CASE WHEN a.grantee=p.proowner THEN '<owner>' ELSE r.rolname END||':'||a.privilege_type ORDER BY 1) AS acl FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a JOIN pg_roles r ON r.oid=a.grantee WHERE p.proname IN('owner_buffered_read_scope','owner_buffered_parity_expected','buffered_sessions_for','buffered_session_stages_for') GROUP BY p.oid,p.proowner")).rows as any[];
 const manifest=(acl as {functions:Record<string,string[]>}).functions;
 const norm=(fn:string)=>fn.replace(/, /g,",");
 expect(rows.length).toBe(4);
 // The parity table: owner-only ACL, RLS on, no rows by default.
 const tbl=(await f.db.query("SELECT c.relrowsecurity rls,(SELECT count(*)::int FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE a.grantee<>c.relowner) others,(SELECT count(*)::int FROM sessions.owner_buffered_parity) n FROM pg_class c WHERE c.oid='sessions.owner_buffered_parity'::regclass")).rows[0];
 expect(tbl).toEqual({rls:true,others:0,n:0});
 for(const r of rows)expect([...r.acl].sort()).toEqual([...manifest[norm(r.fn)]!].sort());
 await f.db.exec(psql("supabase/bootstrap/session_read_api_role_verify.sql"));
});
