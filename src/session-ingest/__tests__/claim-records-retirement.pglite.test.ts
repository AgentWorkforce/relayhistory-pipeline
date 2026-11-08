import {initializeFixtureSeatClock} from "./ordered-support.ts";
import {afterEach,expect,setDefaultTimeout,test} from "bun:test";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {createHash,randomUUID} from "node:crypto";
import {zstdDecompressSync} from "node:zlib";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {objectKey,readAll} from "../../shared/vendor.ts";
import {archiveClaimAttemptRecords,assertCandidate,ClaimRecordsArchiveError} from "../claim-records-archive.ts";
import {scratchStore} from "./support.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
setDefaultTimeout(60000);
const root=join(import.meta.dir,"../../.."),open:Array<()=>unknown>=[];
afterEach(async()=>{for(const f of open.splice(0).reverse())await f()});
const hash=(s:string)=>createHash("sha256").update(s).digest("hex");
const generation="a".repeat(64),tenant={orgId:"org-a",workspaceId:"ws-a",userId:"user-a"},origin="origin-a";
async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());
 const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const n of SESSION_BOOTSTRAP_FILES)await db.exec(read(n));
 for(const n of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+n));
 await db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker','"+generation+"','pass') ON CONFLICT(name) DO UPDATE SET generation=excluded.generation,status='pass'");
 await db.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',repeat('3',64))",[tenant.orgId,generation]);
 await initializeFixtureSeatClock(db as never,generation);
 const fs=scratchStore();open.push(fs.done);
 let serial=0;
 // A batch with one 3-record original claim; staged=false models a canonical-mode attempt.
 async function attempt(o:{staged?:boolean;committed?:boolean;cleanup?:"live"|"retired"|"redacted"}={}){
  const batch="batch-"+(++serial),claim=randomUUID(),token=randomUUID(),digest=hash(batch);
  await db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at,publication_state) VALUES($1,$2,$3,$4,$5,$6,'rhr_'||repeat('a',64),'pending',NULL,$7)",[tenant.orgId,tenant.workspaceId,origin,batch,tenant.userId,digest,o.staged===false?"canonical":"buffered"]);
  const records=[0,1,2].map(ord=>({ord,source:"claude",session_id:"s-"+ord,record_id:"r-"+serial+"-"+ord,revision_id:"r-"+ord+":1",revision:1,digest:hash("r"+serial+ord)}));
  await db.query("INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch,schema_generation) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,0,$10)",[claim,tenant.orgId,tenant.workspaceId,tenant.userId,origin,batch,digest,JSON.stringify(records.map(r=>({source:"claude",session_id:r.session_id,epoch:0,reset_epoch:0}))),JSON.stringify(records),generation]);
  const committedAt="timestamptz '2026-11-04 23:30:00+00'";
  if(o.staged===false){
   if(o.committed!==false)await db.query("UPDATE sessions.ingest_batches SET state='committed',committed_at="+committedAt+" WHERE batch_id=$1",[batch]);
   return {batch,claim,token,records};
  }
  const sha=hash(token),key="96bulls/org-a/buffered-batches/v1/ws=ws-a/m=srv_"+"0".repeat(32)+"/b="+hash(digest).slice(0,32)+"-"+sha.slice(0,16)+"-"+hash(claim).slice(0,32)+".staged.ndjson.zst";
  await db.query("INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,receipt_state,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,10,'[[0],[1],[2]]',0,'preparing',clock_timestamp()+interval '5 minutes')",[token,claim,tenant.orgId,tenant.workspaceId,tenant.userId,origin,batch,digest,key,sha]);
  if(o.committed!==false){
   // Same order as the producer ACK: outbox pointer, acknowledged intent, durable receipt.
   await db.query("INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,origin_id,batch_id,user_id,digest,active_stage_token) VALUES($1,$2,$3,$4,$5,$6,$7)",[tenant.orgId,tenant.workspaceId,origin,batch,tenant.userId,digest,token]);
   await db.query("UPDATE sessions.ingest_stage_intents SET receipt_state='acknowledged' WHERE stage_token=$1",[token]);
   await db.query("UPDATE sessions.ingest_batches SET state='committed',committed_at="+committedAt+",buffer_receipt_kind='staged' WHERE batch_id=$1",[batch]);
   // Later lifecycle (publication/GC swap, then barrier finish) modeled directly.
   if((o.cleanup??"live")!=="live")await db.query("UPDATE sessions.ingest_buffer_outbox SET active_stage_token=NULL,state='erased' WHERE batch_id=$1",[batch]);
  }
  if((o.cleanup??"live")!=="live")await db.query("UPDATE sessions.ingest_stage_intents SET cleanup_state=$2 WHERE stage_token=$1",[token,o.cleanup]);
  return {batch,claim,token,records};
 }
 const sql={query:(s:string,p:unknown[])=>db.query(s,p as never) as never};
 const row=async(claim:string)=>(await db.query("SELECT records,records_sha256,records_count,records_archive_key,records_retired_at IS NOT NULL AS retired FROM sessions.ingest_claim_attempts WHERE claim_token=$1",[claim])).rows[0] as any;
 const candidates=async(limit=100)=>((await db.query("SELECT sessions.ingest_claim_attempt_records_candidates($1::jsonb) r",[JSON.stringify({keyRoot:"96bulls",limit,afterClaimToken:null})])).rows[0] as any).r.candidates as any[];
 return {db,store:fs.store,sql,attempt,row,candidates};
}
const retire=(f:any,p:unknown)=>f.db.query("SELECT sessions.ingest_claim_attempt_records_retire($1::jsonb) r",[JSON.stringify(p)]);

test("only committed attempts with every stage intent redacted (or none) are terminal candidates",async()=>{
 const f=await fixture();
 const live=await f.attempt(),retired=await f.attempt({cleanup:"retired"}),pending=await f.attempt({committed:false,cleanup:"redacted"});
 const redacted=await f.attempt({cleanup:"redacted"}),canonical=await f.attempt({staged:false});
 const got=(await f.candidates()).map(c=>c.claimToken).sort();
 expect(got).toEqual([redacted.claim,canonical.claim].sort());
 for(const c of await f.candidates())assertCandidate(c,"96bulls");
 const c=(await f.candidates()).find(x=>x.claimToken===redacted.claim)!;
 expect(c.recordsCount).toBe(3);expect(JSON.parse(c.recordsText)).toEqual(redacted.records);
 expect(c.archiveKey).toBe("96bulls/org-a/claim-attempt-records/v1/ws=ws-a/d=2026/11/04/c="+hash(redacted.claim).slice(0,32)+"-"+c.recordsSha256.slice(0,16)+".json.zst");
 // Non-terminal attempts are refused under locks even with a correct hash/key.
 for(const a of [live,retired,pending]){
  await expect(retire(f,{keyRoot:"96bulls",claimToken:a.claim,recordsSha256:"0".repeat(64),archiveKey:"x"})).rejects.toMatchObject({code:"40001"});
 }
});
test("dry-run writes nothing; execute archives exact bytes to the org/date key, verifies, retires, and is idempotent",async()=>{
 const f=await fixture(),a=await f.attempt({cleanup:"redacted"}),b=await f.attempt({staged:false}),live=await f.attempt();
 const opts={sql:f.sql,store:f.store,keyRoot:"96bulls",limit:1,maxPages:10};
 const before=JSON.stringify((await f.row(a.claim)).records);
 const dry=await archiveClaimAttemptRecords({...opts,execute:false});
 expect(dry).toMatchObject({examined:2,archived:0,retired:0,already:0});
 expect((await f.row(a.claim)).retired).toBe(false);
 const cands=await f.candidates();
 await expect(f.store.getObject(objectKey(cands[0].archiveKey))).rejects.toBeDefined();
 const run=await archiveClaimAttemptRecords({...opts,execute:true});
 expect(run).toMatchObject({examined:2,archived:2,retired:2,already:0});
 for(const c of cands){
  const r=await f.row(c.claimToken);
  expect(r).toEqual({records:null,records_sha256:c.recordsSha256,records_count:3,records_archive_key:c.archiveKey,retired:true});
  const body=new TextDecoder().decode(zstdDecompressSync(await readAll((await f.store.getObject(objectKey(c.archiveKey))).body)));
  expect(body).toBe(c.recordsText);expect(hash(body)).toBe(c.recordsSha256);
 }
 expect(JSON.parse(cands.find(c=>c.claimToken===a.claim)!.recordsText)).toEqual(JSON.parse(before));
 // Rerun: no candidates remain; the RPC itself is idempotent for the same hash+key.
 expect(await archiveClaimAttemptRecords({...opts,execute:true})).toMatchObject({examined:0,retired:0});
 const c0=cands[0];
 expect((await retire(f,{keyRoot:"96bulls",claimToken:c0.claimToken,recordsSha256:c0.recordsSha256,archiveKey:c0.archiveKey})).rows[0]).toEqual({r:{retired:true,already:true}});
 await expect(retire(f,{keyRoot:"96bulls",claimToken:c0.claimToken,recordsSha256:"f".repeat(64),archiveKey:c0.archiveKey})).rejects.toMatchObject({code:"55000"});
 expect((await f.row(live.claim)).retired).toBe(false);
 expect(cands.map(c=>c.claimToken).sort()).toEqual([a.claim,b.claim].sort());
});
test("hash, key, unknown attempt and archive read-back mismatches refuse before clearing",async()=>{
 const f=await fixture(),a=await f.attempt({cleanup:"redacted"});const c=(await f.candidates())[0];
 await expect(retire(f,{keyRoot:"96bulls",claimToken:a.claim,recordsSha256:"e".repeat(64),archiveKey:c.archiveKey})).rejects.toMatchObject({code:"22023"});
 await expect(retire(f,{keyRoot:"96bulls",claimToken:a.claim,recordsSha256:c.recordsSha256,archiveKey:c.archiveKey.replace("org-a","org-b")})).rejects.toMatchObject({code:"22023"});
 await expect(retire(f,{keyRoot:"96bulls",claimToken:randomUUID(),recordsSha256:c.recordsSha256,archiveKey:c.archiveKey})).rejects.toMatchObject({code:"22023"});
 await expect(retire(f,{keyRoot:"96bulls",claimToken:a.claim,recordsSha256:c.recordsSha256,archiveKey:c.archiveKey,extra:1})).rejects.toBeDefined();
 // A pre-existing different object at the key is never adopted or overwritten.
 await f.store.put(objectKey(c.archiveKey),new Uint8Array([40,181,47,253,0,0,1,0,0,120]),{ifNoneMatch:true});
 await expect(archiveClaimAttemptRecords({sql:f.sql,store:f.store,keyRoot:"96bulls",limit:10,maxPages:1,execute:true})).rejects.toBeDefined();
 expect((await f.row(a.claim)).retired).toBe(false);
 expect(()=>assertCandidate({...c,recordsText:c.recordsText+" "},"96bulls")).toThrow(ClaimRecordsArchiveError);
 expect(()=>assertCandidate({...c,archiveKey:c.archiveKey.replace("/c=","/../c=")},"96bulls")).toThrow(ClaimRecordsArchiveError);
});
test("records clear only by retirement, retirement is final, and a retired attempt can never be reached again",async()=>{
 const f=await fixture(),live=await f.attempt(),a=await f.attempt({cleanup:"redacted"});
 await expect(f.db.query("UPDATE sessions.ingest_claim_attempts SET records=NULL WHERE claim_token=$1",[live.claim])).rejects.toMatchObject({code:"55000"});
 await expect(f.db.query("UPDATE sessions.ingest_claim_attempts SET records=NULL,records_retired_at=now(),records_sha256=repeat('0',64),records_count=3,records_archive_key='k' WHERE claim_token=$1",[live.claim])).rejects.toMatchObject({code:"55000"});
 await expect(f.db.query("UPDATE sessions.ingest_claim_attempts SET records_sha256=repeat('0',64) WHERE claim_token=$1",[live.claim])).rejects.toMatchObject({code:"55000"});
 // Every non-retirement write keeps working exactly as before (existing writers and fixtures).
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET schema_generation=$2 WHERE claim_token=$1",[live.claim,generation]);
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=records WHERE claim_token=$1",[live.claim]);
 await archiveClaimAttemptRecords({sql:f.sql,store:f.store,keyRoot:"96bulls",limit:10,maxPages:1,execute:true});
 await expect(f.db.query("UPDATE sessions.ingest_claim_attempts SET records='[]',records_retired_at=NULL,records_archive_key=NULL WHERE claim_token=$1",[a.claim])).rejects.toBeDefined();
 await expect(f.db.query("UPDATE sessions.ingest_stage_intents SET cleanup_state='live' WHERE stage_token=$1",[a.token])).rejects.toMatchObject({code:"55000"});
 await expect(f.db.query("INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,expires_at) SELECT gen_random_uuid(),claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,'96bulls/org-a/x-'||gen_random_uuid(),repeat('a',64),1,'[[0]]',0,now() FROM sessions.ingest_claim_attempts WHERE claim_token=$1",[a.claim])).rejects.toMatchObject({code:"55000"});
 // Fail closed if any reader were ever pointed at it: original metadata is missing, not empty.
 await expect(f.db.query("SELECT * FROM sessions.ingest_receipt_seat_records($1,'[[0]]')",[a.claim])).rejects.toMatchObject({code:"55000"});
 // A live attempt's readers are unaffected.
 expect((await f.db.query("SELECT count(*)::int n FROM sessions.ingest_receipt_seat_records($1,'[[0],[1],[2]]')",[live.claim])).rows[0]).toEqual({n:3});
});
test("application roles cannot list or retire claim records",async()=>{
 const f=await fixture();await f.attempt({cleanup:"redacted"});
 for(const role of ["bulls96_session_ingest","bulls96_session_ingest_seeder","service_role","authenticated","anon"]){
  for(const fn of ["ingest_claim_attempt_records_candidates","ingest_claim_attempt_records_retire"]){
   await f.db.exec("BEGIN;SET LOCAL ROLE "+role);
   try{await expect(f.db.query("SELECT sessions."+fn+"('{}'::jsonb)")).rejects.toMatchObject({code:"42501"})}finally{await f.db.exec("ROLLBACK")}
  }
 }
});

test("publication-linked original records remain available for identical published replay after stage cleanup",async()=>{
 const f=await fixture(),a=await f.attempt({cleanup:"redacted"});
 const stale=(await f.candidates())[0];
 const attempt=randomUUID(),lease=randomUUID();
 await f.db.query(`INSERT INTO sessions.ingest_publication_attempts(
  attempt_token,schema_generation,stage_token,lease_token,lease_until,stage_version,outbox_version,
  org_id,workspace_id,user_id,origin_id,batch_id,digest,accepted_ordinals,revision_snapshot,head_snapshots,frontier,state,completion_sha256)
 SELECT $1,$2,stage_token,$3,clock_timestamp()+interval '1 minute',row_version,1,
  org_id,workspace_id,user_id,origin_id,batch_id,digest,'[0,1,2]','[]','[]','{}','published',$4
 FROM sessions.ingest_stage_intents WHERE stage_token=$5`,[attempt,generation,lease,hash("[]"),a.token]);
 const replay=async()=>{
  await f.db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
  try{return (await f.db.query("SELECT sessions.ingest_publication_publish($1::jsonb) r",[
   JSON.stringify({attemptToken:attempt,leaseToken:lease,messages:[],generation})])).rows[0] as {r:{records:number;replay:boolean}};}
  finally{await f.db.exec("ROLLBACK")}
 };
 const before=await replay();expect(before.r).toMatchObject({replay:true,records:3});
 // Publication admission is a durable link even after GC/redaction. A stale
 // candidate captured before that link must also fail the locked recheck.
 for(const state of ["admitted","prepared","rejected","published"]){
  await f.db.query("UPDATE sessions.ingest_publication_attempts SET state=$2 WHERE attempt_token=$1",[attempt,state]);
  expect(await f.candidates()).toEqual([]);
  await expect(retire(f,{keyRoot:"96bulls",claimToken:stale.claimToken,recordsSha256:stale.recordsSha256,archiveKey:stale.archiveKey})).rejects.toMatchObject({code:"40001"});
 }
 expect(await archiveClaimAttemptRecords({sql:f.sql,store:f.store,keyRoot:"96bulls",limit:10,maxPages:1,execute:true})).toMatchObject({examined:0,archived:0,retired:0});
 expect(await replay()).toEqual(before);
 expect((await f.row(a.claim)).records).toEqual(a.records);
});
