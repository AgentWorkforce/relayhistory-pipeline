/**
 * Opt-in, owned real PostgreSQL proof of the zero-503 generation overlap (20261105043885): the full schema,
 * the real Worker ledger and deliver path, the owner window, the rebind, and the lock behaviour PGlite cannot
 * show (a close/rebind waits for an in-flight admitted upload instead of deadlocking or cutting it off).
 * SESSION_INGEST_TEST_PG_URL=postgres://postgres@127.0.0.1:<port>/postgres  (a throwaway superuser database)
 */
import {afterEach,describe,test,expect} from "bun:test";
import pg from "pg";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {prepareDelivery} from "../vendor/delivery/prepare.ts";
import {rebindIngestGeneration} from "../../hoopdb/ingest-generation-rebind.ts";
import {openGenerationOverlap,closeGenerationOverlap} from "../../hoopdb/ingest-generation-overlap.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";
import {SESSION_BOOTSTRAP_FILES} from "../../hoopdb/session-bootstrap-chain.ts";
const url=process.env.SESSION_INGEST_TEST_PG_URL;
const root=join(import.meta.dir,"../../..");
const text=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
const A="a".repeat(64),B="f".repeat(64),APPROVAL="9".repeat(64),epoch="3".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
const cleanup:Array<()=>Promise<void>|void>=[];
afterEach(async()=>{for(const f of cleanup.splice(0).reverse())await f();},60000);

async function fixture(){
 const admin=new pg.Client({connectionString:url});await admin.connect();
 const name="gen_overlap_"+crypto.randomUUID().replaceAll("-","");await admin.query("CREATE DATABASE "+name);
 const target=new URL(url!);target.pathname="/"+name;
 const pool=new pg.Pool({connectionString:target.href,max:8});
 cleanup.push(async()=>{await pool.end();await admin.query("DROP DATABASE "+name+" WITH (FORCE)");await admin.end();});
 await pool.query(text("supabase/tests/supabase_shim.sql"));
 await pool.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
 for(const f of SESSION_BOOTSTRAP_FILES)await pool.query(text(f));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await pool.query(text("supabase/migrations/"+f));
 await pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
 await pool.query("INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker',$1,'pass') ON CONFLICT(name) DO UPDATE SET generation=EXCLUDED.generation,status='pass'",[A]);
 const owner=async(fn:string,p:unknown)=>(await pool.query("SELECT sessions."+fn+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0].r;
 await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation:A,sourceIdentitySha256:identity,controlEpoch:epoch,privacyBaselineSha256:baseline,retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
 await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:"9007199254740993",isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:"9007199254740993",generation:A,sourceIdentitySha256:identity,sourceProofSha256:"d".repeat(64)});
 await pool.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",[identity,epoch,baseline,"e".repeat(64)]);
 const run=async(t:string,p:unknown[])=>{const c=await pool.connect();try{await c.query("SET ROLE bulls96_session_ingest");return await c.query(t,p);}finally{await c.query("RESET ROLE");c.release();}};
 const fs=scratchStore();cleanup.push(fs.done);
 const worker=(expected:string,accept?:string[])=>({ledger:sqlLedger(run as never,{acceptanceMode:"buffered",expectedGeneration:expected,...(accept?{acceptGenerations:accept}:{})}),acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}});
 let n=0;
 async function upload(deps:ReturnType<typeof worker>){
  const raw=await batch([record({record_id:"r"+(++n)})],{batch_id:"b"+n});const p=await prepareDelivery(TENANT,raw);
  const custody=(await run("SELECT sessions.ingest_frozen_source_witness($1::jsonb) AS r",[JSON.stringify({kind:"batch",tenant:TENANT,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,originalRecordCount:raw.records.length,
   candidates:p.records.map((r,i)=>({preparedOrd:i,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateDigest:r.digest,source:r.source,sessionId:r.session_id}))})])).rows[0].r;
  return deliverBatch({...deps,custody},TENANT,raw);
 }
 const ok=async(deps:ReturnType<typeof worker>)=>{const r=await upload(deps);expect(r.custodyAck?.sourceDigest).toMatch(/^[a-f0-9]{64}$/);};
 const refusedCode=async(deps:ReturnType<typeof worker>)=>{try{await upload(deps);return null}catch(e){const x=e as {cause_code?:string;code?:string};return x.cause_code??x.code??"?"}};
 const ownerClient=async()=>{const c=await pool.connect();cleanup.push(()=>c.release());return c;};
 const marker=(g:string,s:string)=>pool.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status=$2",[g,s]);
 return{pool,worker,ok,refusedCode,ownerClient,marker};
}

describe.skipIf(!url)("zero-503 generation overlap on real PostgreSQL",()=>{
 test("old Worker admits through pending and PASS of B; {B,A} admits throughout; rebind closes; A refused, B admits",async()=>{
  const f=await fixture();const o=await f.ownerClient();
  const old=f.worker(A),both=f.worker(B,[B,A]),next=f.worker(B);
  await f.ok(both);
  await openGenerationOverlap(o,{from:A,to:B,ttlSeconds:900,admitDuringApply:true,approvalSha256:APPROVAL});
  await f.marker(B,"pending");
  await f.ok(old);await f.ok(both);
  expect(await f.refusedCode(next)).toBe("ledger_sqlstate_55000");
  await f.marker(B,"pass");
  await f.ok(old);await f.ok(both);
  expect(await rebindIngestGeneration(o,{from:A,to:B})).toMatchObject({policy:"rebound",overlapClosed:true});
  expect(await f.refusedCode(old)).toBe("ledger_sqlstate_55000");
  await f.ok(next);await f.ok(both);
 },120000);

 test("an in-flight admitted transaction holds the window: close waits for it, then A is refused; the rebind does not deadlock",async()=>{
  const f=await fixture();const o=await f.ownerClient();
  await openGenerationOverlap(o,{from:A,to:B,ttlSeconds:900,admitDuringApply:true,approvalSha256:APPROVAL});
  await f.marker(B,"pass");
  // Capture the exact claim call a Worker at A makes, then replay it inside a transaction we hold open:
  // an admitted upload in flight (fence passed at A through the window, policy and window rows FOR SHARE).
  let captured:{t:string;p:unknown[]}|null=null;
  const capture=async(t:string,p:unknown[])=>{if(t.includes("ingest_claim_buffered_batch")){captured={t,p};throw Object.assign(new Error("captured"),{code:"57014"});}
   const c=await f.pool.connect();try{await c.query("SET ROLE bulls96_session_ingest");return await c.query(t,p);}finally{await c.query("RESET ROLE");c.release();}};
  const raw=await batch([record({record_id:"inflight"})],{batch_id:"inflight"});const p=await prepareDelivery(TENANT,raw);
  const w=(await capture("SELECT sessions.ingest_frozen_source_witness($1::jsonb) AS r",[JSON.stringify({kind:"batch",tenant:TENANT,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,originalRecordCount:1,
   candidates:p.records.map((r,i)=>({preparedOrd:i,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateDigest:r.digest,source:r.source,sessionId:r.session_id}))})])).rows[0].r;
  await deliverBatch({ledger:sqlLedger(capture as never,{acceptanceMode:"buffered",expectedGeneration:A}),acceptanceMode:"buffered",target:{store:scratchStore().store,keyRoot:"96bulls"},custody:w} as never,TENANT,raw).catch(()=>undefined);
  expect(captured).not.toBeNull();
  const tx=await f.pool.connect();cleanup.push(()=>tx.release());
  await tx.query("BEGIN");await tx.query("SET LOCAL ROLE bulls96_session_ingest");
  const claim=(await tx.query(captured!.t,captured!.p)).rows[0].result;
  expect(claim.kind).toBe("claimed");
  // The rebind must wait (lock_timeout would surface 55P03), not deadlock (40P01) and not cut the transaction off.
  const o2=await f.ownerClient();
  let settled=false;
  const rebind=rebindIngestGeneration(o2,{from:A,to:B}).then((r)=>{settled=true;return r;});
  await new Promise((r)=>setTimeout(r,500));
  expect(settled).toBe(false);
  await tx.query("COMMIT");
  expect(await rebind).toMatchObject({policy:"rebound",overlapClosed:true});
  expect(await f.refusedCode(f.worker(A))).toBe("ledger_sqlstate_55000");
  await f.ok(f.worker(B));
  expect(await closeGenerationOverlap(o,{from:A,to:B,reason:"owner"})).toEqual({closed:false});
 },120000);

 test("an upload admitted during the apply never locks the readiness marker: the migrator's PASS write does not wait on it",async()=>{
  const f=await fixture();const o=await f.ownerClient();
  await openGenerationOverlap(o,{from:A,to:B,ttlSeconds:900,admitDuringApply:true,approvalSha256:APPROVAL});
  await f.marker(B,"pending");
  let captured:{t:string;p:unknown[]}|null=null;
  const capture=async(t:string,p:unknown[])=>{if(t.includes("ingest_claim_buffered_batch")){captured={t,p};throw Object.assign(new Error("captured"),{code:"57014"});}
   const c=await f.pool.connect();try{await c.query("SET ROLE bulls96_session_ingest");return await c.query(t,p);}finally{await c.query("RESET ROLE");c.release();}};
  const raw=await batch([record({record_id:"during-apply"})],{batch_id:"during-apply"});const p=await prepareDelivery(TENANT,raw);
  const w=(await capture("SELECT sessions.ingest_frozen_source_witness($1::jsonb) AS r",[JSON.stringify({kind:"batch",tenant:TENANT,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,originalRecordCount:1,
   candidates:p.records.map((r,i)=>({preparedOrd:i,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateDigest:r.digest,source:r.source,sessionId:r.session_id}))})])).rows[0].r;
  await deliverBatch({ledger:sqlLedger(capture as never,{acceptanceMode:"buffered",expectedGeneration:A}),acceptanceMode:"buffered",target:{store:scratchStore().store,keyRoot:"96bulls"},custody:w} as never,TENANT,raw).catch(()=>undefined);
  const tx=await f.pool.connect();cleanup.push(()=>tx.release());
  await tx.query("BEGIN");await tx.query("SET LOCAL ROLE bulls96_session_ingest");
  expect((await tx.query(captured!.t,captured!.p)).rows[0].result.kind).toBe("claimed");
  // Phase 2's PASS write (migrate.ts) under a short lock_timeout: it must not wait on the admitted upload.
  const m=await f.ownerClient();
  await m.query("BEGIN");await m.query("SET LOCAL lock_timeout='1s'");
  await m.query("UPDATE sessions.__96bulls_role_validation SET status='pass' WHERE name='session_directory_search_worker' AND generation=$1 AND status='pending'",[B]);
  await m.query("COMMIT");
  await tx.query("COMMIT");
  await f.ok(f.worker(A));
 },120000);
});
