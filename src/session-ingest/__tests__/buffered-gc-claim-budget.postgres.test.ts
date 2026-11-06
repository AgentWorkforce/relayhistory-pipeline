/**
 * Opt-in owned PostgreSQL reproduction of buffered_gc_sql_57014: a 500-record,
 * lazy-current healthy backlog makes one claim(5) (20 candidates, one statement)
 * exceed the job's per-operation statement_timeout. The timeout rolls back every
 * gc_checked_at rotation, so the identical claim fails again on the next run.
 * The single-ticket tick commits each 4-candidate claim and makes progress under
 * the SAME bounded executor and budget. Synthetic local database only.
 */
import {afterAll,beforeAll,describe,expect,test} from "bun:test";
import pg from "pg";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {prepareDelivery} from "../vendor/delivery/prepare.ts";
import {SemanticPostgresExecutor} from "../../search-features/semantic-copy-postgres.ts";
import {bufferedGcTick,sqlBufferedGcLedger} from "../buffered-gc.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";
const url=process.env.SESSION_INGEST_TEST_PG_URL;
const root=join(import.meta.dir,"../../.."),generation="a".repeat(64),epoch="3".repeat(64);
const text=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
const BATCHES=20,RECORDS=500;
describe.skipIf(!url)("buffered GC claim budget on a large lazy-current backlog",()=>{
 let admin:pg.Client,pool:pg.Pool,gcPool:pg.Pool,name:string,done:()=>void;
 beforeAll(async()=>{
  admin=new pg.Client({connectionString:url});await admin.connect();
  name="gc_budget_"+crypto.randomUUID().replaceAll("-","");await admin.query("CREATE DATABASE "+name);
  const target=new URL(url!);target.pathname="/"+name;
  pool=new pg.Pool({connectionString:target.href,max:3});
  // Job-shaped pool: dedicated ingest role, connect<=1s, bounded executor below.
  gcPool=new pg.Pool({connectionString:target.href,max:2,connectionTimeoutMillis:1000,options:"-c role=bulls96_session_ingest"});
  await pool.query(text("supabase/tests/supabase_shim.sql"));await pool.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
  for(const f of ["session_directory_search_worker.sql","session_token_verifier.sql","session_ingest_roles.sql"])await pool.query(text("supabase/bootstrap/"+f));
  for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await pool.query(text("supabase/migrations/"+f));
  await pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text)");
  await pool.query("INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker',$1,'pass') ON CONFLICT(name) DO UPDATE SET generation=excluded.generation,status='pass'",[generation]);
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
  }
  await pool.query("ANALYZE");
 },300000);
 afterAll(async()=>{await gcPool?.end();await pool?.end();done?.();if(admin){await admin.query("DROP DATABASE IF EXISTS "+name+" WITH(FORCE)");await admin.end()}},60000);
 // Exactly the job's per-operation context: deadline=min(job,now+budget) per RPC.
 const jobLedger=(budgetMs:number)=>{
  const sql=new SemanticPostgresExecutor(gcPool),signal=new AbortController().signal;
  return sqlBufferedGcLedger({query:(s,p)=>sql.queryBounded(s,p,{signal,deadline:Date.now()+budgetMs})},generation);
 };
 const rotated=async()=>(await pool.query("SELECT count(*)::int AS n FROM sessions.ingest_stage_intents WHERE gc_checked_at IS NOT NULL")).rows[0].n as number;
 const unrotate=()=>pool.query("UPDATE sessions.ingest_stage_intents SET gc_checked_at=NULL");
 test("claim(5) times out with 57014 every run and makes no progress; single-ticket tick commits progress under the same budget",async()=>{
  expect((await pool.query("SELECT count(*)::int AS n,min(jsonb_array_length(group_ordinals))::int AS g FROM sessions.ingest_stage_intents WHERE cleanup_state='live' AND receipt_state='acknowledged'")).rows[0]).toEqual({n:BATCHES,g:RECORDS});
  // Calibrate on this machine: the cost of one 4-candidate claim(1).
  const samples:number[]=[];
  for(let i=0;i<5;i++){
   await unrotate();const started=performance.now();
   expect(await jobLedger(60000).claim(1)).toEqual([]);
   samples.push(performance.now()-started);expect(await rotated()).toBe(4);
  }
  const four=samples.sort((a,b)=>a-b)[2]!;
  const allTwenty=async()=>{await unrotate();const s=performance.now();expect(await jobLedger(60000).claim(5)).toEqual([]);return performance.now()-s};
  const twenty=[await allTwenty(),await allTwenty(),await allTwenty()].sort((a,b)=>a-b)[1]!;
  // The single statement over 20 heavy candidates costs ~5x one 4-candidate claim.
  expect(twenty).toBeGreaterThan(3*four);
  // Budget between them, as production's 1.8 s sits between its two costs.
  const budget=Math.ceil(Math.sqrt(four*twenty));
  await unrotate();
  for(let runNo=0;runNo<3;runNo++){
   await expect(jobLedger(budget).claim(5)).rejects.toMatchObject({code:"sql_57014"});
   expect(await rotated()).toBe(0); // rolled back: the same head is retried forever
  }
  // The changed tick, same executor and same per-RPC budget, never touches S3 for
  // a healthy backlog (no tickets), and durably rotates all 20 candidates.
  const store=new Proxy({},{get(){throw Error("store_not_expected")}}) as never;
  const eraser={eraseObjectExcept:async()=>{throw Error("eraser_not_expected")}};
  expect(await bufferedGcTick({ledger:jobLedger(budget),store,eraser,keyRoot:"96bulls"},5)).toEqual({examined:0,finished:0,deferred:0});
  expect(await rotated()).toBe(BATCHES);
  console.log(JSON.stringify({gcClaimBudgetProof:{fourCandidateMs:Math.round(four),twentyCandidateMs:Math.round(twenty),budgetMs:budget}}));
 },300000);
 test("an erasable candidate behind the healthy backlog is reached and leased by later single-ticket claims",async()=>{
  await unrotate();
  const d=await pool.query("SELECT stage_token FROM sessions.ingest_stage_intents ORDER BY created_at DESC LIMIT 1");
  // Make the newest object unreferenced (acknowledged without outbox pointer) => cleanup ticket.
  await pool.query("ALTER TABLE sessions.ingest_buffer_outbox DISABLE TRIGGER USER");
  await pool.query("UPDATE sessions.ingest_buffer_outbox SET active_stage_token=NULL,state='erased' WHERE active_stage_token=$1",[d.rows[0].stage_token]);
  await pool.query("ALTER TABLE sessions.ingest_buffer_outbox ENABLE TRIGGER USER");
  const ledger=jobLedger(60000);const seen:string[]=[];
  for(let i=0;i<BATCHES/4&&!seen.length;i++)for(const t of await ledger.claim(1))seen.push(t.stageToken+":"+t.phase+":"+t.cleanupState);
  expect(seen).toEqual([d.rows[0].stage_token+":cleanup:retired"]);
 },120000);
});
