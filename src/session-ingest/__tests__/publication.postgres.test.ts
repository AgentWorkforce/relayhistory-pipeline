
import {afterEach,describe,test,expect} from "bun:test";
import pg from "pg";
import {hoopDbSessionIndex}from "../../hoopdb/index.ts";
import {inTransaction,type HoopDb}from "../../shared/db.ts";
import {indexPointer,droppedFanout}from "../../session-pipeline/pointer.ts";
import {objectStoreManifestReader,prefixOrgResolver,readAll,objectKey,parseSessionKey,rawChunkKey}from "../../shared/vendor.ts";
import {readFileSync,readdirSync,mkdtempSync,rmSync} from "node:fs";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {zstdDecompressSync}from "node:zlib";
import {createHash}from "node:crypto";
import {createFsStore} from "../../../packages/object-store/src/fs.ts";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {batch,record,TENANT} from "./support.ts";
import {orderedFixtureLedger,initializeFixtureSeatClock}from "./ordered-support.ts";
import {publicationRpc} from "../publication-rpc.ts";
import {canonicalPublicationTick,publishBufferedStage} from "../publication-job.ts";
import {layoutComponent} from "../../shared/layout-component.ts";
import type {ObjectStore} from "../../shared/vendor.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
const url=process.env.SESSION_INGEST_TEST_PG_URL;
const GEN="a".repeat(64);
const FRONTIER_CAPTURE="4".repeat(64),FRONTIER_PLAN="5".repeat(64),FRONTIER_EVIDENCE="6".repeat(64);
const CANARY_PLAN_ID="77777777-7777-4777-8777-777777777777";
const root=join(import.meta.dir,"../../..");
const read=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
const clean:Array<()=>Promise<void>>=[];
afterEach(async()=>{for(const f of clean.splice(0).reverse())await f();},30000);
async function fixture(frontier=true,initialRecords=[record({record_id:"new-r1"})],guard=false){
 const admin=new pg.Client({connectionString:url});await admin.connect();const name="publisher_"+crypto.randomUUID().replaceAll("-","");
 await admin.query("CREATE DATABASE "+name);const u=new URL(url!);u.pathname="/"+name;
 const pool=new pg.Pool({connectionString:u.href});const dir=mkdtempSync(join(tmpdir(),"publisher-"));
 clean.push(async()=>{await pool.end();await admin.query("DROP DATABASE "+name);await admin.end();rmSync(dir,{recursive:true,force:true});});
 await pool.query(read("supabase/tests/supabase_shim.sql"));await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
 for(const f of SESSION_BOOTSTRAP_FILES)await pool.query(read(f));
 for(const f of readdirSync(join(root,"supabase/migrations")).sort())await pool.query(read("supabase/migrations/"+f));
 for(const f of readdirSync(join(root,"src/hoopdb/migrations")).sort())await pool.query(read("src/hoopdb/migrations/"+f));
 await pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
 await pool.query("INSERT INTO sessions.__96bulls_role_validation(name,generation,status)VALUES('session_directory_search_worker',$1,'pass')",[GEN]);
 await pool.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)VALUES($1,$2,$2,$3,clock_timestamp()+interval'1 hour',repeat('3',64))",[TENANT.orgId,"a".repeat(64),GEN]);
 await initializeFixtureSeatClock(pool,GEN);
 if(frontier)await pool.query("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,'origin-1',$4,$4,clock_timestamp()+interval'1 hour','synthetic','us-east-1','96bulls',$5)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,"b".repeat(64),GEN]);
 if(guard){
  await pool.query(read("src/session-ingest/__tests__/fixtures/publication-canary-guard.sql"));
  if(frontier)await pool.query(`INSERT INTO sessions.proof_publication_frontier_attestations(
   org_id,workspace_id,user_id,origin_id,capture_id,frontier_plan_sha256,frontier_evidence_sha256,
   source_frontier_sha256,record_maxima_sha256,valid_until,target_bucket,target_region,key_root,schema_generation)
   SELECT org_id,workspace_id,user_id,origin_id,$1,$2,$3,source_frontier_sha256,record_maxima_sha256,
    valid_until,target_bucket,target_region,key_root,schema_generation FROM sessions.ingest_publication_frontiers`,
   [FRONTIER_CAPTURE,FRONTIER_PLAN,FRONTIER_EVIDENCE]);
 }
 const run=async(t:string,p:unknown[])=>{const c=await pool.connect();try{await c.query("SET ROLE bulls96_session_ingest");return await c.query(t,p);}finally{await c.query("RESET ROLE");c.release();}};
 const ledger=orderedFixtureLedger(sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:GEN}),pool);
 const store=createFsStore({root:dir}) as unknown as ObjectStore;
 await deliverBatch({ledger,target:{store,keyRoot:"96bulls"},acceptanceMode:"buffered"},TENANT,await batch(initialRecords));
 const stage=(await pool.query("SELECT active_stage_token token FROM sessions.ingest_buffer_outbox")).rows[0].token as string;
 const call=async(fn:string,p:unknown)=>(await run("SELECT sessions."+fn+"($1::jsonb) result",[JSON.stringify(fn.startsWith("ingest_publication_")||fn==="ingest_buffer_gc_claim"?{...p as any,generation:GEN}:p)])).rows[0].result;
 const promote=async(t:any)=>{
  const h=t.heads[0],key=h.prefix+"/v1/ws="+h.physicalWorkspace+"/m="+h.machineId+"/h="+h.physicalSource+"/d=2026/11/01/s="+h.physicalSession+"/publications/"+t.attemptToken+"/manifest.json";
  const sha="f".repeat(64);
  await pool.query("UPDATE sessions.ingest_publication_heads SET state='live',current_key=$1,current_raw_sha256=$2,pointer_revision=$3 WHERE generation=$4",[key,sha,h.revision,h.generation]);
  await pool.query("UPDATE sessions.ingest_publication_generations SET state='live'WHERE generation=$1",[h.generation]);
  const row={org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,user_id:TENANT.userId,harness:h.physicalSource,session_id:h.physicalSession,machine_id:h.machineId,created_at:"2026-11-01T00:00:00Z",last_activity_at:"2026-11-01T00:00:00Z",storage_provider:"s3",bucket:"synthetic",manifest_key:key,content_sha256:"\\x"+sha,raw_format:"ndjson",revision:Number(h.revision),trace_id:"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18"};
  await pool.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)",[JSON.stringify([row])]);
  return {key,row,evidence:{orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,harness:h.physicalSource,sessionId:h.physicalSession,machineId:h.machineId,manifestKey:key,revision:h.revision,schemaGeneration:GEN}};
 };
 const claim=()=>call("ingest_publication_claim",{stageToken:stage,keyRoot:"96bulls",bucket:"synthetic",region:"us-east-1"});
 const target={bucket:"synthetic",region:"us-east-1",keyRoot:"96bulls",generation:GEN};
 const rpc=publicationRpc({queryBounded:async(q:string,p:unknown[],ctx:{signal:AbortSignal})=>{ctx.signal.throwIfAborted();return run(q,p);}}as never,target);
 const canaryCreatedAt=new Date(Date.now()-1000).toISOString(),canaryExpiresAt=new Date(Date.now()+60_000).toISOString();
 const inspect=()=>call("proof_publication_canary_inspect",{stageToken:stage,bucket:target.bucket,region:target.region,keyRoot:target.keyRoot,generation:GEN});
 const guarded=(inspection:any,overrides:Record<string,unknown>={})=>{
  const ownerId=String(overrides.ownerId??"offline-managed-canary"),planId=String(overrides.planId??CANARY_PLAN_ID);
  const expiresAt=String(overrides.expiresAt??canaryExpiresAt);
  const planJson=JSON.stringify({version:1,planId,ownerId,createdAt:canaryCreatedAt,expiresAt,
   tenant:{orgId:inspection.expected.tenant.orgId,workspaceId:inspection.expected.tenant.workspaceId},target,
   limits:{maxStages:1,maxRecordsPerStage:10,maxSessionsPerStage:4,maxStageMs:30000,maxPlanMs:60000,maxReads:128,maxReadBytes:33554432,maxPuts:128,maxPutBytes:33554432},
   stages:[{stageToken:inspection.expected.stageToken,tenant:{orgId:inspection.expected.tenant.orgId,workspaceId:inspection.expected.tenant.workspaceId},
    userId:inspection.expected.tenant.userId,originId:inspection.expected.originId,sha256:inspection.expected.stage.sha256,bytes:Number(inspection.expected.stage.bytes)}]});
  return call("proof_publication_canary_claim",{planId,ownerId,planJson,planDigest:createHash("sha256").update(planJson).digest("hex"),expiresAt,
   inspectionSha256:inspection.evidenceDigest,expected:inspection.expected,bucket:target.bucket,region:target.region,keyRoot:target.keyRoot,generation:GEN,
   ...Object.fromEntries(Object.entries(overrides).filter(([key])=>!["ownerId","planId","expiresAt"].includes(key)))});
 };
 return {pool,run,ledger,store,stage,call,claim,promote,target,rpc,inspect,guarded};
}
describe.skipIf(!url)("background publisher actual PostgreSQL admission",()=>{
 test("managed canary inspect and guarded claim bind exact owner-plan evidence with no ordinary-claim bypass",async()=>{
  const d=await fixture(true,[record({record_id:"guarded-r1"})],true);
  await expect(d.run("SELECT count(*) FROM sessions.ingest_stage_intents",[])).rejects.toMatchObject({code:"42501"});
  await expect(d.claim()).rejects.toMatchObject({code:"42501"});
  const inspected=await d.inspect();expect(inspected).toMatchObject({decision:"eligible",expected:{
   stageToken:d.stage,tenant:{orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,userId:TENANT.userId},originId:"origin-1",
   frontier:{captureId:FRONTIER_CAPTURE,planSha256:FRONTIER_PLAN,evidenceSha256:FRONTIER_EVIDENCE}}});
  await expect(d.guarded(inspected,{planJson:"{}"})).rejects.toMatchObject({code:"22023"});
  const claimed=await d.guarded(inspected);expect(claimed.binding).toMatchObject({
   planId:CANARY_PLAN_ID,ownerId:"offline-managed-canary",stageToken:d.stage,evidenceDigest:inspected.evidenceDigest});
  expect(claimed.binding.planDigest).toMatch(/^[a-f0-9]{64}$/);
  expect(claimed.ticket).toMatchObject({stageToken:d.stage,tenant:{...TENANT},originId:"origin-1",schemaGeneration:GEN});
  const retried=await d.guarded(inspected);expect(retried.ticket.attemptToken).toBe(claimed.ticket.attemptToken);
  await expect(d.guarded(inspected,{ownerId:"different-owner"})).rejects.toMatchObject({code:"40001"});
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.proof_publication_canary_bindings WHERE attempt_token=$1",[claimed.ticket.attemptToken])).rows[0].n).toBe(1);
 },15000);
 test("guarded claim mismatch rolls back binding and every ordinary-claim side effect",async()=>{
  const d=await fixture(true,[record({record_id:"guard-mismatch"})],true),inspected=await d.inspect();
  const forged=structuredClone(inspected);forged.expected.stage.sha256="e".repeat(64);
  await expect(d.guarded(forged)).rejects.toMatchObject({code:"40001"});
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.proof_publication_canary_bindings")).rows[0].n).toBe(0);
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.ingest_publication_attempts")).rows[0].n).toBe(0);
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.ingest_publication_heads")).rows[0].n).toBe(0);
  expect((await d.pool.query("SELECT publication_state FROM sessions.ingest_batches")).rows[0].publication_state).toBe("buffered");
 },15000);
 test("frontier mutation waits behind guarded binding and invalidates the resulting lease",async()=>{
  const d=await fixture(true,[record({record_id:"guard-race"})],true),inspected=await d.inspect();
  await d.pool.query(`CREATE FUNCTION public.pause_canary_binding()RETURNS trigger LANGUAGE plpgsql AS $p$
   BEGIN PERFORM pg_advisory_xact_lock(960600);RETURN NEW;END$p$;
   CREATE TRIGGER pause_canary_binding BEFORE INSERT ON sessions.proof_publication_canary_bindings
   FOR EACH ROW EXECUTE FUNCTION public.pause_canary_binding()`);
  const gate=await d.pool.connect();await gate.query("BEGIN;SELECT pg_advisory_xact_lock(960600)");
  const claiming=d.guarded(inspected);
  let waiting=false;
  for(let i=0;i<100&&!waiting;i++){
   const q=await d.pool.query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE '%proof_publication_canary_claim%') waiting");
   waiting=q.rows[0].waiting;if(!waiting)await new Promise(r=>setTimeout(r,10));
  }
  expect(waiting).toBe(true);
  const mutation=d.pool.query("UPDATE sessions.ingest_publication_frontiers SET source_frontier_sha256=$1",["e".repeat(64)]);
  await new Promise(r=>setTimeout(r,50));await gate.query("COMMIT");gate.release();
  const claimed=await claiming;await mutation;
  expect(claimed.ticket.stageToken).toBe(d.stage);
  await expect(d.rpc.heartbeat(claimed.ticket,new AbortController().signal)).rejects.toMatchObject({code:"55000"});
  expect((await d.pool.query("SELECT source_frontier_sha256 FROM sessions.ingest_publication_frontiers")).rows[0].source_frontier_sha256).toBe("e".repeat(64));
 },15000);
 test("frontier absence refuses despite a durable buffered receipt",async()=>{
  const d=await fixture(false);await expect(d.claim()).rejects.toMatchObject({code:"55000"});
  expect((await d.pool.query("SELECT publication_state FROM sessions.ingest_batches")).rows[0].publication_state).toBe("buffered");
 },15000);
 test("complete all-record refilter closes baseline before image writes and leases stage against GC",async()=>{
  const d=await fixture();
  await d.pool.query("INSERT INTO sessions.ingest_seed_fence_baselines(org_id,workspace_id,user_id,fingerprint)VALUES($1,$2,$3,$4)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,"c".repeat(64)]);
  const ticket=await d.claim();
  expect(ticket.acceptedOrdinals).toEqual([0]);expect(ticket.heads).toHaveLength(1);
  expect(ticket.receiptSeats.records).toHaveLength(1);
  expect(ticket.receiptSeats.records[0]).toMatchObject({ord:0,bodyAccepted:true,candidateDigest:ticket.records[0].digest});
  expect(ticket.receiptSeats.records[0].seat).toMatch(/^[1-9][0-9]*$/);
  expect(ticket.heads[0].prefix).toContain("/upload-generations/g=");expect(ticket.heads[0].revision).toBe("1");
  expect((await d.pool.query("SELECT publication_state FROM sessions.ingest_batches")).rows[0].publication_state).toBe("canonical");
  expect((await d.pool.query("SELECT closed_at IS NOT NULL yes FROM sessions.ingest_seed_fence_baselines")).rows[0].yes).toBe(true);
  expect((await d.call("ingest_buffer_gc_claim",{limit:10})).tickets).toEqual([]);
  await expect(d.call("ingest_buffer_gc_prepare",{stageToken:d.stage,leaseToken:ticket.leaseToken,key:"none",sha256:"a".repeat(64),bytes:1,groupOrdinals:[[0]]})).rejects.toMatchObject({code:"40001"});
 },15000);
 test("strict maxima refilter can drop every stale ordinal without creating an empty image",async()=>{
  const d=await fixture();await d.pool.query("INSERT INTO sessions.ingest_record_revisions(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id)VALUES($1,$2,'origin-1','new-r1',7,'known-r7',$3,$4,'claude','sess-1')",[TENANT.orgId,TENANT.workspaceId,"d".repeat(64),TENANT.userId]);
  const ticket=await d.claim();expect(ticket.acceptedOrdinals).toEqual([]);expect(ticket.heads).toEqual([]);
 },15000);
 test("a later equal-revision first-ACK loser is excluded before heads or a host ticket can be poisoned",async()=>{
  const winner=record({record_id:"shared-native",session_id:"session-A",payload:{text:"first body",ts_ms:1761955200000}});
  const d=await fixture(true,[winner]);
  const loser=record({record_id:"shared-native",source:"codex",session_id:"session-B",payload:{text:"later body",ts_ms:1761955200000}});
  await deliverBatch({ledger:d.ledger,target:{store:d.store,keyRoot:"96bulls"},acceptanceMode:"buffered"},TENANT,await batch([loser],{batch_id:"losing-batch"}));
  const stage=(await d.pool.query("SELECT active_stage_token token FROM sessions.ingest_buffer_outbox WHERE batch_id='losing-batch'")).rows[0].token;
  const ticket=await d.call("ingest_publication_claim",{stageToken:stage,keyRoot:"96bulls",bucket:"synthetic",region:"us-east-1"});
  expect(ticket.receiptSeats.records[0].bodyAccepted).toBe(false);
  expect(ticket.acceptedOrdinals).toEqual([]);expect(ticket.heads).toEqual([]);
 },15000);
 test("changed source frontier or original privacy epoch refuses a borrowed heartbeat",async()=>{
  const d=await fixture();const t=await d.claim();
  await d.pool.query("UPDATE sessions.ingest_publication_frontiers SET record_maxima_sha256=$1",["e".repeat(64)]);
  await expect(d.call("ingest_publication_heartbeat",{attemptToken:t.attemptToken,leaseToken:t.leaseToken})).rejects.toMatchObject({code:"55000"});
  await d.pool.query("UPDATE sessions.ingest_publication_frontiers SET record_maxima_sha256=$1",["b".repeat(64)]);
  await d.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:false});
  await d.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:true});
  await expect(d.call("ingest_publication_heartbeat",{attemptToken:t.attemptToken,leaseToken:t.leaseToken})).rejects.toMatchObject({code:"40001"});
 },15000);
 test("lease expiry after a held intent lock cannot be renewed",async()=>{
  const d=await fixture();const t=await d.claim();const lock=await d.pool.connect();await lock.query("BEGIN");
  await lock.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()+interval'0.15 seconds'WHERE stage_token=$1",[d.stage]);
  const renewing=d.call("ingest_publication_heartbeat",{attemptToken:t.attemptToken,leaseToken:t.leaseToken});
  await new Promise(r=>setTimeout(r,250));await lock.query("COMMIT");lock.release();
  await expect(renewing).rejects.toMatchObject({code:"40001"});
 },15000);
 test("managed pointer SQL rejects old-key higher revisions and unqualified delayed classification",async()=>{
  const d=await fixture();const t=await d.claim();const image=await d.promote(t);
  const old={...image.row,manifest_key:"96bulls/org-a/v1/ws=ws-a/m="+image.row.machine_id+"/h=claude/d=2026/11/01/s=sess-1/manifest.json",revision:999999};
  const out=await d.pool.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)",[JSON.stringify([old])]);
  expect(out.rows[0].applied).toBe(false);
  expect((await d.pool.query("SELECT manifest_key FROM sessions.session_index")).rows[0].manifest_key).toBe(image.key);
  expect((await d.pool.query("SELECT sessions.set_session_state($1,$2,$3,$4,$5,'done',clock_timestamp()) ok",[TENANT.orgId,TENANT.workspaceId,"claude","sess-1",image.row.machine_id])).rows[0].ok).toBe(false);
  const classify=async(key:string)=>(await d.pool.query("SELECT sessions.set_session_state_for_publication($1,$2,$3,$4,$5,$6,$7,'active',clock_timestamp(),$8) ok",[TENANT.orgId,TENANT.workspaceId,"claude","sess-1",image.row.machine_id,key,t.heads[0].revision,GEN])).rows[0].ok;
  expect(await classify(old.manifest_key)).toBe(false);expect(await classify(image.key)).toBe(true);
 },15000);
 test("disable/enable retires permanent authority and a stale higher revision cannot recreate it",async()=>{
  const d=await fixture();const t=await d.claim();const image=await d.promote(t);
  await d.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:false});
  await d.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:true});
  expect((await d.pool.query("SELECT state FROM sessions.ingest_publication_generations")).rows[0].state).toBe("retired");
  expect((await d.pool.query("SELECT deleted_at IS NOT NULL yes FROM sessions.session_index")).rows[0].yes).toBe(true);
  await d.pool.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)",[JSON.stringify([{...image.row,revision:999999}])]);
  expect((await d.pool.query("SELECT deleted_at IS NOT NULL yes FROM sessions.session_index")).rows[0].yes).toBe(true);
  const probe=(await d.pool.query("SELECT sessions.session_publication_probe($1::jsonb) result",[JSON.stringify(image.evidence)])).rows[0].result;
  expect(probe.managed).toBe(true);expect(probe.current).toBe(false);
 },15000);
 test("actual buffered stage drains through shared codec, atomic image/pointer/revision/job CAS and leaves buffer cleanup durable",async()=>{
  const d=await fixture();
  const result=await publishBufferedStage({rpc:d.rpc,target:d.target,store:()=>d.store},d.stage,{enabled:true,maxJobMs:10000,heartbeatMs:100});
  expect(result.enabled).toBe(true);
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.ingest_record_revisions")).rows[0].n).toBe(1);
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.pipeline_jobs WHERE stage='object-landed'")).rows[0].n).toBe(1);
  const p=(await d.pool.query("SELECT *FROM sessions.session_index")).rows[0];expect(p.manifest_key).toContain("/publications/");
  expect((await d.store.head(p.manifest_key)).size).toBeGreaterThan(0);
  const manifest=(await objectStoreManifestReader(d.store,{provider:"s3",bucket:"synthetic"}).read(p.manifest_key))!.manifest;
  const location=parseSessionKey(p.manifest_key)!;const chunk=manifest.raw.chunks.at(-1)!;
  const stored=await readAll(await d.store.get(rawChunkKey(location,chunk.seq,chunk.start_offset,chunk.sha256,"ndjson")));
  const published=JSON.parse(new TextDecoder().decode(zstdDecompressSync(stored)).trim());
  expect(published.accepted_seq).toBe((await d.pool.query("SELECT s.seat::text FROM sessions.ingest_receipt_ord_seats o JOIN sessions.ingest_receipt_seats s ON s.key_sha256=o.key_sha256")).rows[0].seat);
  expect((await d.pool.query("SELECT state,active_stage_token FROM sessions.ingest_buffer_outbox")).rows[0]).toMatchObject({state:"erased",active_stage_token:null});
  expect((await d.pool.query("SELECT cleanup_state FROM sessions.ingest_stage_intents")).rows[0].cleanup_state).toBe("retired");
  expect((await d.pool.query("SELECT state FROM sessions.ingest_publication_attempts")).rows[0].state).toBe("published");
 },15000);
 test("layout SQL exactly matches shared JS including astral and long identities",async()=>{
  const d=await fixture();
  for(const id of["plain",".leading","sess:1","emoji😀mix","é"+"x".repeat(200),"x".repeat(127)+"😀z"]){
   const out=await d.pool.query("SELECT sessions.ingest_publication_component($1) val",[id]);expect(out.rows[0].val).toBe(layoutComponent(id));
  }
 },15000);
 test("target-filtered candidates cannot adopt a foreign bucket or schema frontier",async()=>{
  const d=await fixture();
  expect((await d.call("ingest_publication_candidates",{limit:10,bucket:"other",region:"us-east-1",keyRoot:"96bulls"})).stageTokens).toEqual([]);
  expect(await d.rpc.candidates(10,new AbortController().signal)).toEqual([d.stage]);
 },15000);
 test("failed first image keeps registered keys erasable, retires its generation, and retry chooses fresh generation",async()=>{
  const d=await fixture();
  const broken=new Proxy(d.store,{get(target,key){if(key==="put")return async()=>{throw Error("synthetic_put_failure")};const v=Reflect.get(target,key);return typeof v==="function"?v.bind(target):v;}});
  await expect(publishBufferedStage({rpc:d.rpc,target:d.target,store:()=>broken},d.stage,{enabled:true,maxJobMs:10000})).rejects.toMatchObject({code:"publication_job_failed"});
  const old=(await d.pool.query("SELECT generation,state FROM sessions.ingest_publication_generations")).rows[0];expect(old.state).toBe("retired");
  const gc=await d.call("ingest_publication_gc_claim",{limit:10,bucket:"synthetic",region:"us-east-1",keyRoot:"96bulls"});
  expect(gc.tickets.length).toBeGreaterThan(0);expect(gc.tickets.every((t:any)=>t.uploadGeneration===old.generation)).toBe(true);
  expect((await d.call("ingest_publication_gc_claim",{limit:10,bucket:"other",region:"us-east-1",keyRoot:"96bulls"})).tickets).toEqual([]);
  const retry=await d.claim();expect(retry.heads[0].generation).not.toBe(old.generation);
  const ticket=gc.tickets[0];
  expect((await d.call("ingest_publication_gc_finish",{key:ticket.key,leaseToken:ticket.leaseToken,rowVersion:ticket.rowVersion,barrierVersionId:"synthetic-version"})).finished).toBe(true);
  expect((await d.pool.query("SELECT state FROM sessions.ingest_publication_generations WHERE generation=$1",[retry.heads[0].generation])).rows[0].state).toBe("building");
 },15000);
 test("expired crashed builder is retired by GC and cannot heartbeat or publish after lease reclamation",async()=>{
  const d=await fixture(),t=await d.claim();
  await d.pool.query("UPDATE sessions.ingest_publication_heads SET build_lease_until=clock_timestamp()-interval'1 second'");
  await d.pool.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval'1 second'");
  await d.call("ingest_publication_gc_claim",{limit:10,bucket:"synthetic",region:"us-east-1",keyRoot:"96bulls"});
  expect((await d.pool.query("SELECT state FROM sessions.ingest_publication_generations")).rows[0].state).toBe("retired");
  await expect(d.rpc.heartbeat(t,new AbortController().signal)).rejects.toMatchObject({code:"40001"});
  const retry=await d.claim();expect(retry.heads[0].generation).not.toBe(t.heads[0].generation);
 },15000);

 test("managed indexer commits ext, directory and all three fanout jobs under final same-transaction authority",async()=>{
  const d=await fixture();await publishBufferedStage({rpc:d.rpc,target:d.target,store:()=>d.store},d.stage,{enabled:true,maxJobMs:10000});
  const row=(await d.pool.query("SELECT*FROM sessions.session_index")).rows[0];
  const reader=objectStoreManifestReader(d.store,{provider:"s3",bucket:"synthetic"});const m=(await reader.read(row.manifest_key))!.manifest;
  let externalQueue=0;const db={pool:d.pool,query:async(text:string,args?:unknown[])=>d.pool.query(text,args)}as HoopDb;
  const index=hoopDbSessionIndex({db,reader,queue:{backend:"memory",send:async()=>{externalQueue++},sendBatch:async()=>{externalQueue++}},current:()=>undefined,brief:null,
   publication:{schemaGeneration:GEN,transact:work=>inTransaction(db,work)}});
  await index.upsertRawPointer({traceId:m.trace_id,workspaceId:row.workspace_id,harness:row.harness,sessionId:row.session_id,machineId:row.machine_id,manifestKey:row.manifest_key,provider:"s3",bucket:"synthetic"},
   {traceId:m.trace_id,orgId:row.org_id,workspaceId:row.workspace_id,harness:row.harness,sessionId:row.session_id,machineId:row.machine_id});
  expect(externalQueue).toBe(0);expect((await d.pool.query("SELECT count(*)::int n FROM sessions.session_index_ext")).rows[0].n).toBe(1);
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.pipeline_jobs WHERE stage IN('summary-short','summary-long','status-classify')")).rows[0].n).toBe(3);
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.session_directory WHERE deleted_at IS NULL")).rows[0].n).toBe(1);
 },15000);
 test("privacy transition after indexer hydration prevents ext/directory/fanout resurrection",async()=>{
  const d=await fixture();await publishBufferedStage({rpc:d.rpc,target:d.target,store:()=>d.store},d.stage,{enabled:true,maxJobMs:10000});
  const row=(await d.pool.query("SELECT*FROM sessions.session_index")).rows[0],base=objectStoreManifestReader(d.store,{provider:"s3",bucket:"synthetic"});
  const db={pool:d.pool,query:async(text:string,args?:unknown[])=>d.pool.query(text,args)}as HoopDb;
  const reader={...base,async read(key:string){const r=await base.read(key);
   await d.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:false});
   await d.ledger.setSessionUploads({tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],enabled:true});return r;}};
  const index=hoopDbSessionIndex({db,reader,queue:{backend:"memory",send:async()=>{throw Error("external_queue")},sendBatch:async()=>{}},current:()=>undefined,brief:null,
   publication:{schemaGeneration:GEN,transact:work=>inTransaction(db,work)}});
  const traceId="019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
  const outcome=await index.upsertRawPointer({traceId,workspaceId:row.workspace_id,harness:row.harness,sessionId:row.session_id,machineId:row.machine_id,manifestKey:row.manifest_key,provider:"s3",bucket:"synthetic"},
   {traceId,orgId:row.org_id,workspaceId:row.workspace_id,harness:row.harness,sessionId:row.session_id,machineId:row.machine_id});
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.session_index_ext")).rows[0].n).toBe(0);
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.pipeline_jobs WHERE stage IN('summary-short','summary-long','status-classify')")).rows[0].n).toBe(0);
  expect((await d.pool.query("SELECT deleted_at IS NOT NULL gone FROM sessions.session_index")).rows[0].gone).toBe(true);
  expect(outcome).toMatchObject({status:"skipped",reason:"publication_retired"});
 },15000);

 test("a heartbeat paused after authority capture cannot extend an expired original stage lease",async()=>{
  const d=await fixture(),t=await d.claim();
  await d.pool.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()+interval'0.15 second'");
  await d.pool.query("CREATE FUNCTION public.pause_heartbeat()RETURNS trigger LANGUAGE plpgsql AS $p$BEGIN IF NEW.gc_lease_until>OLD.gc_lease_until THEN PERFORM pg_sleep(0.25);END IF;RETURN NEW;END$p$;CREATE TRIGGER pause_heartbeat BEFORE UPDATE ON sessions.ingest_stage_intents FOR EACH ROW EXECUTE FUNCTION public.pause_heartbeat()");
  await expect(d.rpc.heartbeat(t,new AbortController().signal)).rejects.toMatchObject({code:"40001"});
  expect((await d.pool.query("SELECT gc_lease_until<clock_timestamp() expired FROM sessions.ingest_stage_intents")).rows[0].expired).toBe(true);
  expect((await d.pool.query("SELECT lease_until=$1::timestamptz same FROM sessions.ingest_publication_attempts",[t.leaseUntil])).rows[0].same).toBe(true);
 },15000);

 test("canonical admission never reopens original buffered nonce for old direct or queue-fanout writes",async()=>{
  const d=await fixture(),t=await d.claim();
  expect((await d.pool.query("SELECT publication_state FROM sessions.ingest_batches")).rows[0].publication_state).toBe("canonical");
  for(const fn of["ingest_fanout_fence","ingest_commit_revisions","ingest_commit_batch"])
   await expect(d.call(fn,{tenant:TENANT,originId:"origin-1",sessions:[["claude","sess-1"]],batchDigest:t.batchDigest,claimToken:t.claimToken})).rejects.toMatchObject({code:"55000"});
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.ingest_record_revisions")).rows[0].n).toBe(0);
 },15000);

 test("a durable policy-held batch is backed off without starving a subsequent healthy upload",async()=>{
  const d=await fixture(true,[record({record_id:"held-move",revision:8})]);
  await d.pool.query("INSERT INTO sessions.ingest_record_revisions(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id)VALUES($1,$2,'origin-1','held-move',7,'historical-r7',$3,$4,'codex','historical-session')",[TENANT.orgId,TENANT.workspaceId,"d".repeat(64),TENANT.userId]);
  await deliverBatch({ledger:d.ledger,target:{store:d.store,keyRoot:"96bulls"},acceptanceMode:"buffered"},TENANT,await batch([record({record_id:"healthy-new"})],{batch_id:"healthy-batch"}));
  const deps={rpc:d.rpc,target:d.target,store:()=>d.store};
  const first=await canonicalPublicationTick(deps,{enabled:true,candidateLimit:1,maxJobMs:10000});expect(first.deferred).toBe(1);expect(first.published).toBe(0);
  const second=await canonicalPublicationTick(deps,{enabled:true,candidateLimit:1,maxJobMs:10000});expect(second.published).toBe(1);
  expect((await d.pool.query("SELECT state,active_stage_token FROM sessions.ingest_buffer_outbox WHERE batch_id='batch-1'")).rows[0]).toMatchObject({state:"buffered",active_stage_token:d.stage});
  expect((await d.pool.query("SELECT reason,retry_after>clock_timestamp() waiting FROM sessions.ingest_publication_deferrals")).rows[0]).toMatchObject({reason:"55000",waiting:true});
  expect((await d.pool.query("SELECT revision FROM sessions.ingest_record_revisions WHERE record_id='held-move'")).rows[0].revision).toBe("7");
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.ingest_record_revisions WHERE record_id='healthy-new'")).rows[0].n).toBe(1);
 },15000);

 test("no-session records remain durable archival carriers with reset erasure and zero public/model rows",async()=>{
  const d=await fixture(true,[record({record_id:"archival-presence",kind:"presence",session_id:null})]);
  const r=await publishBufferedStage({rpc:d.rpc,target:d.target,store:()=>d.store},d.stage,{enabled:true,maxJobMs:10000});
  expect(r).toMatchObject({enabled:true,images:1,records:1,eligibleSessions:0});
  for(const name of["session_index","session_directory","pipeline_jobs"])
   expect((await d.pool.query("SELECT count(*)::int n FROM sessions."+name)).rows[0].n).toBe(0);
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.ingest_record_revisions")).rows[0].n).toBe(1);
  await d.ledger.resetUploadedSessions(TENANT,"reset-archival-case");
  expect((await d.pool.query("SELECT state FROM sessions.ingest_publication_generations")).rows[0].state).toBe("retired");
  expect((await d.call("ingest_publication_gc_claim",{limit:10,bucket:"synthetic",region:"us-east-1",keyRoot:"96bulls"})).tickets.length).toBeGreaterThan(0);
 },15000);

 test("legacy pointer event losing the first-takeover race cannot write ext/directory/fanout after an earlier hint read",async()=>{
  const d=await fixture();await publishBufferedStage({rpc:d.rpc,target:d.target,store:()=>d.store},d.stage,{enabled:true,maxJobMs:10000});
  const current=(await d.pool.query("SELECT*FROM sessions.session_index")).rows[0];
  const oldKey=current.manifest_key.replace(/\/upload-generations\/g=[^/]+/,'').replace(/\/publications\/[^/]+\/manifest.json$/,'/manifest.json');
  const manifest=JSON.parse(new TextDecoder().decode(await readAll(await d.store.get(objectKey(current.manifest_key)))));delete manifest.extensions['96bulls.upload_publication'];
  await d.store.put(objectKey(oldKey),new TextEncoder().encode(JSON.stringify(manifest)),{});
  const db={pool:d.pool,query:async(text:string,args?:unknown[])=>text.includes('session_publication_is_managed')?{rows:[{managed:false}]}:d.pool.query(text,args)}as HoopDb;
  const outcome=await indexPointer({key:oldKey,provider:'s3',bucket:'synthetic'},
   {db,reader:objectStoreManifestReader(d.store,{provider:'s3',bucket:'synthetic'}),lock:async(_scope,work)=>work(),exists:async key=>!!await d.store.head(objectKey(key)),orgs:prefixOrgResolver,allowPrefix:()=>true,fanout:droppedFanout,
    publication:{schemaGeneration:GEN,transact:work=>inTransaction(db,work)}}).catch(error=>({error:error.code}));
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.session_index_ext")).rows[0].n).toBe(0);
  expect((await d.pool.query("SELECT count(*)::int n FROM sessions.pipeline_jobs WHERE stage IN('summary-short','summary-long','status-classify')")).rows[0].n).toBe(0);
  expect(outcome).toEqual({status:'ignored',reason:'managed_publication_hint'});
  expect((await d.pool.query("SELECT manifest_key FROM sessions.session_index")).rows[0].manifest_key).toBe(current.manifest_key);
 },15000);

});
