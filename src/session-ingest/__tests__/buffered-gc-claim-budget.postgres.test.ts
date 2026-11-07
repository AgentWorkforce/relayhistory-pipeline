/** Owned PG17 counterfactuals at the actual 1800 ms SQL operation bound.
 * Fixture-only transaction-local gates distinguish historical #264 bulk versus
 * separate claims and current 039 one-candidate exits, without host timing ratios.
 * Old SQL and controlled mutations exist only in the owned DB and are restored.
 */
import {afterAll,beforeAll,describe,expect,test} from "bun:test";
import pg from "pg";
import {readFileSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {prepareDelivery} from "../vendor/delivery/prepare.ts";
import {SemanticPostgresExecutor} from "../../search-features/semantic-copy-postgres.ts";
import {bufferedGcTick,sqlBufferedGcLedger} from "../buffered-gc.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";
import {runMigrations} from "../../hoopdb/migrate.ts";
import {derivedSchemaAssets} from "../../session-pipeline/derived-schema.ts";
import {schemaVerdict,type Transact} from "../../shared/db.ts";
const url=process.env.SESSION_INGEST_TEST_PG_URL;
if(url){
 const u=new URL(url);
 if(!["postgres:","postgresql:"].includes(u.protocol)||!["127.0.0.1","localhost","[::1]"].includes(u.hostname)||
  u.username!=="postgres"||u.password||u.pathname!=="/postgres"||u.search||u.hash)throw Error("owned_loopback_fixture_required");
}
const root=join(import.meta.dir,"../../.."),epoch="3".repeat(64);
let generation:string;
const text=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
const BATCHES=20,RECORDS=500,OPERATION_MS=1800;
const claimDefinition=(sql:string)=>{
 const match=sql.match(/CREATE OR REPLACE FUNCTION sessions\.ingest_buffer_gc_claim\(p jsonb\)[\s\S]*?END \$claim\$;/);
 if(!match)throw Error("fixture_claim_definition_missing");return match[0];
};
const originalClaim=claimDefinition(text("supabase/migrations/20261104055000_lazy_current_custody.sql"));
const currentClaim=claimDefinition(text("supabase/migrations/20261105039000_buffered_gc_single_candidate.sql"));
describe.skipIf(!url)("buffered GC claim budget on a large lazy-current backlog",()=>{
 let admin:pg.Client,pool:pg.Pool,gcPool:pg.Pool,name:string,done:()=>void;
 const failures:Array<{code?:string;where?:string}>=[];
 const transact:Transact=async work=>{const c=await pool.connect();try{await c.query("BEGIN");const r=await work(c as never);await c.query("COMMIT");return r}finally{await c.query("ROLLBACK").catch(()=>{});c.release()}};
 beforeAll(async()=>{
  admin=new pg.Client({connectionString:url});await admin.connect();
  const version=Number((await admin.query("SHOW server_version_num")).rows[0].server_version_num);
  expect(version).toBeGreaterThanOrEqual(170000);expect(version).toBeLessThan(180000);
  expect((await admin.query("SHOW server_encoding")).rows[0].server_encoding).toBe("UTF8");
  name="gc_budget_"+crypto.randomUUID().replaceAll("-","");await admin.query("CREATE DATABASE "+name);
  const target=new URL(url!);target.pathname="/"+name;
  pool=new pg.Pool({connectionString:target.href,max:3});
  // Job-shaped pool: dedicated ingest role, connect<=1s, bounded executor below.
  gcPool=new pg.Pool({connectionString:target.href,max:2,connectionTimeoutMillis:1000,options:"-c role=bulls96_session_ingest"});
  await pool.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
  const migrationClient=await pool.connect();
  try{await runMigrations(migrationClient,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}})}finally{migrationClient.release()}
  const assets=await derivedSchemaAssets();generation=assets.expected.generation;
  expect(await schemaVerdict({...assets.expected,dbMode:"local"},transact)).toEqual({ok:true});
  await new SemanticPostgresExecutor(gcPool,true).queryBounded("SET LOCAL hoopdb.db_mode='local';"+Object.values(assets.verifiers).join("\n"),[],{signal:new AbortController().signal,deadline:Date.now()+OPERATION_MS});
  console.log(JSON.stringify({proof:"gc-budget-current-generation",generation,migrations:assets.expected.names.length,operationMs:OPERATION_MS,fourConsumerVerifiers:"pass"}));
  const owner=(fn:string,p:unknown)=>pool.query("SELECT sessions."+fn+"($1::jsonb)",[JSON.stringify(p)]);
  await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation,sourceIdentitySha256:"c".repeat(64),controlEpoch:epoch,privacyBaselineSha256:"a".repeat(64),retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
  await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:"9007199254740993",isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:"9007199254740993",generation,sourceIdentitySha256:"c".repeat(64),sourceProofSha256:"d".repeat(64)});
  const run=async(s:string,p:unknown[]=[])=>{const c=await pool.connect();try{await c.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest");const r=await c.query(s,p as never);await c.query("COMMIT");return r as never}catch(e){await c.query("ROLLBACK");throw e}finally{c.release()}};
  const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation});
  const fs=scratchStore();done=fs.done;
  const sign=async(witness:unknown)=>({witness,sourceDigest:(await pool.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d",[JSON.stringify(witness)])).rows[0].d as string});
  // Actual producer path: lazy-current custody, first-ACK seats and ACK decisions.
  for(let b=0;b<BATCHES;b++){
   const raw=await batch(Array.from({length:RECORDS},(_,i)=>record({record_id:"budget-"+b+"-"+i,session_id:"budget-session-"+b+"-"+i,payload:{text:"synthetic budget message "+i}})),{batch_id:"budget-"+b});
   const p=await prepareDelivery(TENANT,raw);
   const custody=await sign({version:1,issuer:"relayhistory-cloud",sourceIdentitySha256:"c".repeat(64),controlEpoch:epoch,privacyBaselineSha256:"a".repeat(64),tenant:TENANT,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,sourceControlVersion:"0",pending:false,pendingControl:null,privacyVersion:"0",originalRecordCount:raw.records.length,candidateCount:p.records.length,records:p.records.map((r,i)=>({preparedOrd:i,processingRank:i,candidateDigest:r.digest,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateFenced:false,currentFenced:false,current:null})),privacy:{complete:true,deletions:[],resets:[]}});
   const ack=await deliverBatch({ledger,acceptanceMode:"buffered",target:{store:fs.store,keyRoot:"96bulls"},custody} as never,TENANT,raw);
   expect(ack.outcome).toBe("accepted_buffered");
   await pool.query("UPDATE sessions.ingest_stage_intents SET created_at='2000-01-01'::timestamptz+$2::int*interval '1 second' WHERE batch_id=$1",["budget-"+b,b]);
  }
  await pool.query("ANALYZE");
 },300000);
 afterAll(async()=>{await gcPool?.end();await pool?.end();done?.();if(admin){if(name)await admin.query("DROP DATABASE IF EXISTS "+name+" WITH(FORCE)");await admin.end()}},60000);
 // The production operation bound is fixed; no calibrated or 60-second claim.
 const jobLedger=()=>{
  const sql=new SemanticPostgresExecutor(gcPool),signal=new AbortController().signal;
  return sqlBufferedGcLedger({query:async(s,p)=>{
   try{return await sql.queryBounded(s,p,{signal,deadline:Date.now()+OPERATION_MS})}
   catch(e){failures.push(e as never);throw e}
  }},generation);
 };
 const rotated=async()=>(await pool.query("SELECT count(*)::int AS n FROM sessions.ingest_stage_intents WHERE gc_checked_at IS NOT NULL")).rows[0].n as number;
 const unrotate=async()=>{
  await pool.query("UPDATE sessions.ingest_stage_intents SET gc_checked_at=NULL");
  expect((await pool.query("SELECT batch_id FROM sessions.ingest_stage_intents ORDER BY coalesce(gc_checked_at,created_at),stage_token")).rows.map(r=>r.batch_id)).toEqual(Array.from({length:BATCHES},(_,i)=>"budget-"+i));
 };
 const dropGate=()=>pool.query("DROP TRIGGER IF EXISTS gc_budget_fixture_gate ON sessions.ingest_stage_intents;DROP FUNCTION IF EXISTS sessions.__gc_budget_fixture_gate()");
 const gate=async(threshold:2|5)=>{
  await dropGate();
  // This owner-only synthetic trigger delays the Nth healthy rotation in ONE
  // transaction. The custom GUC is LOCAL and resets at COMMIT/ROLLBACK. Null
  // fixture reset updates never count. No production source or deadline changes.
  await pool.query(`CREATE FUNCTION sessions.__gc_budget_fixture_gate() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
   DECLARE n int;BEGIN
    n:=coalesce(nullif(current_setting('gc_budget_fixture.rotations',true),''),'0')::int+1;
    PERFORM set_config('gc_budget_fixture.rotations',n::text,true);
    IF n>=${threshold} THEN PERFORM pg_sleep(10);END IF;RETURN NEW;
   END $$;
   CREATE TRIGGER gc_budget_fixture_gate AFTER UPDATE OF gc_checked_at ON sessions.ingest_stage_intents FOR EACH ROW
    WHEN(NEW.gc_checked_at IS NOT NULL AND NEW.gc_checked_at IS DISTINCT FROM OLD.gc_checked_at) EXECUTE FUNCTION sessions.__gc_budget_fixture_gate()`);
 };
 const gateTimeout=async(work:()=>Promise<unknown>)=>{
  const before=failures.length;
  await expect(work()).rejects.toMatchObject({code:"sql_57014"});
  expect(failures.length).toBe(before+1);
  expect(failures.at(-1)?.where).toContain("__gc_budget_fixture_gate"); // causal gate, not unrelated host slowness
  expect(await rotated()).toBe(0);
 };
 const noIo=()=>({ledger:jobLedger(),store:new Proxy({},{get(){throw Error("store_not_expected")}}) as never,
  eraser:{eraseObjectExcept:async()=>{throw Error("eraser_not_expected")}},keyRoot:"96bulls" as const});
 const emptyTick={examined:0,finished:0,deferred:0};
 test("historical #264: exact G2 bulk claim times out and rolls back; five separate claims durably rotate twenty at the same 1800 ms bound",async()=>{
  expect((await pool.query("SELECT count(*)::int AS n,min(jsonb_array_length(group_ordinals))::int AS g FROM sessions.ingest_stage_intents WHERE cleanup_state='live' AND receipt_state='acknowledged'")).rows[0]).toEqual({n:BATCHES,g:RECORDS});
  try{
   await pool.query(originalClaim);await gate(5);await unrotate();
   for(let i=0;i<3;i++)await gateTimeout(()=>jobLedger().claim(5));
   // The old aggregate implementation of the tick is exactly this failing
   // claim(5). The released #264 loop makes five independent claim(1) commits.
   expect(await bufferedGcTick(noIo(),5)).toEqual(emptyTick);
   expect(await rotated()).toBe(BATCHES);
  }finally{await dropGate();await pool.query(currentClaim)}
  console.log(JSON.stringify({proof:"historical-264-controlled-g2",operationMs:OPERATION_MS,bulkTimeouts:3,bulkCommittedRotations:0,separateCommittedRotations:BATCHES,noObjectIo:true}));
 },30000);
 test("039 claim limits1/5/100 each commit one rotation; old G2 and a missing healthy EXIT fail the causal second-rotation gate",async()=>{
  try{
   await gate(2);
   for(const limit of [1,5,100]){
    await unrotate();expect(await jobLedger().claim(limit)).toEqual([]);expect(await rotated()).toBe(1);
    expect((await pool.query("SELECT count(*)::int AS n FROM sessions.ingest_stage_intents WHERE gc_lease_token IS NOT NULL")).rows[0].n).toBe(0);
   }
   await unrotate();await pool.query(originalClaim);await gateTimeout(()=>jobLedger().claim(1));
   const healthyExit="     EXIT;\n    ELSE v_phase:='plan';END IF;";
   expect(currentClaim.split(healthyExit)).toHaveLength(2);
   const missingExit=currentClaim.replace(healthyExit,"     CONTINUE;\n    ELSE v_phase:='plan';END IF;");
   await pool.query(missingExit);await unrotate();await gateTimeout(()=>jobLedger().claim(1));
  }finally{await dropGate();await pool.query(currentClaim)}
  console.log(JSON.stringify({proof:"039-one-candidate-causal-controls",operationMs:OPERATION_MS,limits:[1,5,100],rotationsPerCall:1,oldG2AndMissingExitRefused:true}));
 },20000);
 test("039 default-five tick commits five healthy checks; four ticks cover twenty with zero object IO",async()=>{
  await unrotate();
  for(let i=1;i<=4;i++){
   expect(await bufferedGcTick(noIo(),5)).toEqual(emptyTick);expect(await rotated()).toBe(5*i);
  }
 });
 test("039 reaches the actual privacy-fenced tail after nineteen healthy candidates within twenty bounded calls",async()=>{
  await unrotate();const last=(await pool.query("SELECT stage_token,claim_token FROM sessions.ingest_stage_intents WHERE batch_id=$1",["budget-"+(BATCHES-1)])).rows[0];
  const c=await pool.connect();
  try{
   await c.query("BEGIN");
   await c.query("SELECT sessions.ingest_apply_selection($1,$2,$3,'origin-1',$4::jsonb,false)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,JSON.stringify([["claude",`budget-session-${BATCHES-1}-0`]])]);
   await c.query("SELECT sessions.ingest_lazy_clear($1::jsonb,'selection')",[JSON.stringify({tenant:TENANT,originId:"origin-1",sessions:[["claude",`budget-session-${BATCHES-1}-0`]],enabled:false})]);
   await c.query("COMMIT");
  }finally{await c.query("ROLLBACK").catch(()=>{});c.release()}
  const ledger=jobLedger();
  for(let i=0;i<BATCHES-1;i++){expect(await ledger.claim(1)).toEqual([]);expect(await rotated()).toBe(i+1)}
  const tickets=await ledger.claim(1);expect(tickets).toHaveLength(1);
  expect(tickets[0]).toMatchObject({stageToken:last.stage_token,claimToken:last.claim_token,receiptState:"acknowledged",phase:"plan",cleanupState:"live"});
  expect(tickets[0]!.groupOrdinals).toHaveLength(RECORDS);
  expect(tickets[0]!.keepOrdinals).toEqual(Array.from({length:RECORDS-1},(_,i)=>i+1));
  expect(tickets[0]!.keepGroupIndexes).toEqual(Array.from({length:RECORDS-1},(_,i)=>i+1));
  expect(await rotated()).toBe(BATCHES);
  expect((await pool.query("SELECT buffer_receipt_kind,state FROM sessions.ingest_batches WHERE batch_id=$1",["budget-"+(BATCHES-1)])).rows[0]).toEqual({buffer_receipt_kind:"staged",state:"committed"});
  console.log(JSON.stringify({proof:"039-quiescent-tail-progress",operationMs:OPERATION_MS,calls:BATCHES,healthyRotations:BATCHES-1,leasedTail:true,retainedOrdinals:RECORDS-1}));
 });
});
