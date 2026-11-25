
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
import {parsePublicationTicket,publicationRpc} from "../publication-rpc.ts";
import type {PublicationRpc,PublicationTicket} from "../publication-protocol.ts";
import {canonicalPublicationTick,publishBufferedStage} from "../publication-job.ts";
import {lazyAckT2FlipOutcome,type FlipResult} from "../lazy-ack-t2-flip.ts";
import {layoutComponent} from "../../shared/layout-component.ts";
import type {ObjectStore} from "../../shared/vendor.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import {VersionedMemoryStore} from "./versioned-store.ts";
import {heldPublicationSentinels,publicationKernelSlot,publicationRecoverySlot} from "../../../supabase/proposals/continuous-publication/held-profile.ts";
const url=process.env.SESSION_INGEST_TEST_PG_URL;
const orderedTemplate=process.env.PUBLICATION_PROOF_TEMPLATE === "publication_release_fixture";
const searchDown="supabase/rollbacks/20261105062300_search_index_stage.down.sql";
const GEN="a".repeat(64);
const FRONTIER_CAPTURE="4".repeat(64),FRONTIER_PLAN="5".repeat(64),FRONTIER_EVIDENCE="6".repeat(64);
const CANARY_PLAN_ID="77777777-7777-4777-8777-777777777777";
const root=join(import.meta.dir,"../../..");
const read=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
const clean:Array<()=>Promise<void>>=[];
afterEach(async()=>{for(const f of clean.splice(0).reverse())await f();},30000);
async function fixture(frontier=true,initialRecords=[record({record_id:"new-r1"})],guard=false, storeOverride?: ObjectStore){
 const admin=new pg.Client({connectionString:url});await admin.connect();const name="publisher_"+crypto.randomUUID().replaceAll("-","");
 await admin.query("CREATE DATABASE "+name+(orderedTemplate ? " TEMPLATE publication_release_fixture" : ""));const u=new URL(url!);u.pathname="/"+name;
 const pool=new pg.Pool({connectionString:u.href});const dir=mkdtempSync(join(tmpdir(),"publisher-"));
 clean.push(async()=>{await pool.end();await admin.query("DROP DATABASE "+name);await admin.end();rmSync(dir,{recursive:true,force:true});});
 if (!orderedTemplate) {
 await pool.query(read("supabase/tests/supabase_shim.sql"));await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
 for(const f of SESSION_BOOTSTRAP_FILES)await pool.query(read(f));
 for(const f of readdirSync(join(root,"supabase/migrations")).sort())await pool.query(read("supabase/migrations/"+f));
 for(const f of readdirSync(join(root,"src/hoopdb/migrations")).sort())await pool.query(read("src/hoopdb/migrations/"+f));
 }
 await pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
 await pool.query("INSERT INTO sessions.__96bulls_role_validation(name,generation,status)VALUES('session_directory_search_worker',$1,'pass') ON CONFLICT (name) DO UPDATE SET generation=EXCLUDED.generation,status=EXCLUDED.status",[GEN]);
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
 const store=storeOverride ?? createFsStore({root:dir}) as unknown as ObjectStore;
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
 const target={bucket:"synthetic",region:"us-east-1",keyRoot:"96bulls",generation:GEN,provider:"s3" as const};
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
 test("062210: the real publisher and an upload complete while try-lock T2 flips run back to back (no queued exclusive, no stall)",async()=>{
  const d=await fixture();
  await d.pool.query("SELECT sessions.lazy_ack_t2_set_gate('fenced')");
  let stop=false;const flips:FlipResult[]=[];let queued=0;
  const flipper=(async()=>{let to:"main"|"t2"="t2";
   while(!stop){const r=await lazyAckT2FlipOutcome((q,p)=>d.pool.query(q,p as never[])as never,to,{budgetMs:50});flips.push(r);if(r.result==="switched")to=to==="t2"?"main":"t2";
    queued=Math.max(queued,Number((await d.pool.query("SELECT count(*) n FROM pg_locks WHERE locktype='advisory' AND NOT granted AND objid=50000")).rows[0].n));}})();
  const t0=performance.now();
  let published:any,uploaded:any;
  try{[published,uploaded]=await Promise.all([
   publishBufferedStage({rpc:d.rpc,target:d.target,store:()=>d.store},d.stage,{enabled:true,maxJobMs:10000,heartbeatMs:100}).then((r)=>({r,ms:performance.now()-t0})),
   deliverBatch({ledger:d.ledger,target:{store:d.store,keyRoot:"96bulls"},acceptanceMode:"buffered"},TENANT,await batch([record({record_id:"t2-flip-r2"})],{batch_id:"t2-flip-b2"})).then((r)=>({r,ms:performance.now()-t0})),
  ]);}finally{stop=true;await flipper;}
  console.log(JSON.stringify({case:"062210 publisher under flips",publishMs:Math.round(published.ms),uploadMs:Math.round(uploaded.ms),
   flips:{tries:flips.length,switched:flips.filter((f)=>f.result==="switched").length,noSwitch:flips.filter((f)=>f.result==="no_switch").length},maxQueuedOn50000:queued}));
  expect(published.r.enabled).toBe(true);
  expect((await d.pool.query("SELECT state FROM sessions.ingest_publication_attempts")).rows[0].state).toBe("published");
  expect(uploaded.r.outcome).toBe("accepted_buffered");
  expect(flips.every((f)=>f.result!=="indeterminate")).toBe(true);
  expect(flips.length).toBeGreaterThan(0);
  // A successful try holds 50000 exclusive for one autocommit statement; nothing ever queues behind a FAILED try.
  expect(published.ms).toBeLessThan(10000);
 },30000);
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

describe.skipIf(!url)("scoped v1 real PostgreSQL qualification", () => {
  async function scopedFixture() {
    const d = await fixture();
    const binding = {planSha256:"c".repeat(64), admissionId:CANARY_PLAN_ID, frontierSha256:"e".repeat(64),
      tenant:{...TENANT,originId:"origin-1"},target:d.target};
    await d.pool.query(read("src/session-ingest/__tests__/fixtures/publication-scoped-v1.sql"));
    await d.pool.query(`INSERT INTO sessions.fixture_scoped_admission(id,binding,stage,frontier_image,admission_image,stage_sha)
      SELECT $1::uuid,$2::jsonb,s.stage_token,to_jsonb(f),to_jsonb(a),s.stage_sha256 FROM sessions.ingest_stage_intents s
      JOIN sessions.ingest_publication_frontiers f USING(org_id,workspace_id,user_id,origin_id)
      JOIN sessions.ingest_buffer_admission a USING(org_id) WHERE s.stage_token=$3`,
      [binding.admissionId,JSON.stringify(binding),d.stage]);
    const conn = { generation:GEN, keyRoot:d.target.keyRoot, region:d.target.region, bucket:d.target.bucket, binding };
    return { d, binding, conn };
  }
  test("dedicated LOGIN cannot bypass; restoring ordinary ingest grants still leaves the nested role guard unresolved", async () => {
    const { d,conn }=await scopedFixture();
    const role="pub_fixture_"+crypto.randomUUID().replaceAll("-","");
    await d.pool.query("CREATE ROLE "+role+" LOGIN NOINHERIT");
    clean.push(async()=>{await d.pool.query("DROP OWNED BY "+role);await d.pool.query("DROP ROLE "+role);});
    await d.pool.query("GRANT USAGE ON SCHEMA sessions TO "+role);
    for (const op of ["candidates","claim","publish"]) {
      await d.pool.query("GRANT EXECUTE ON FUNCTION sessions.ingest_publication_scoped_"+op+"_v1(jsonb) TO "+role);
      // Simulate retaining the historical grants for the existing runtime while a separate login is evaluated.
      await d.pool.query("GRANT EXECUTE ON FUNCTION sessions.ingest_publication_"+op+"(jsonb) TO bulls96_session_ingest");
    }
    const dbName=(await d.pool.query("select current_database() db")).rows[0].db;
    const u=new URL(url!);u.pathname="/"+dbName;u.username=role;u.password="";
    const c=new pg.Client({connectionString:u.href});await c.connect();
    try {
      expect((await c.query("SELECT session_user AS login, current_user AS role")).rows[0]).toEqual({login:role,role});
      await expect(c.query("SET ROLE bulls96_session_ingest")).rejects.toMatchObject({code:"42501"});
      await expect(c.query("SELECT * FROM sessions.fixture_scoped_admission")).rejects.toMatchObject({code:"42501"});
      await expect(c.query("SELECT sessions.ingest_publication_claim('{}'::jsonb)")).rejects.toMatchObject({code:"42501"});
      await expect(c.query("SELECT sessions.ingest_publication_scoped_candidates_v1($1)",[JSON.stringify({...conn,limit:1})]))
        .rejects.toMatchObject({code:"42501",message:"session ingest: caller is not the function's role"});
      const live=await d.call("ingest_publication_candidates",{keyRoot:"96bulls",bucket:"synthetic",region:"us-east-1",limit:1});
      expect(live.stageTokens).toEqual([d.stage]);
    } finally {await c.end();}
  },30000);

  test("a committed claim survives connection loss and lease expiry without issuing a second ticket or accepting a final", async()=>{
    const {d,conn,binding}=await scopedFixture();
    const claimant=await d.pool.connect();
    let ticket;
    try {
      await claimant.query("SET ROLE bulls96_session_ingest");
      ticket=(await claimant.query("SELECT sessions.ingest_publication_scoped_claim_v1($1) AS ticket",
        [JSON.stringify({...conn,stageToken:d.stage})])).rows[0].ticket;
    } finally {claimant.release(true);}
    await d.pool.query("UPDATE sessions.ingest_publication_attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE attempt_token=$1",[ticket.attemptToken]);
    await d.pool.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[d.stage]);
    expect(await d.call("ingest_publication_scoped_candidates_v1",{...conn,limit:1})).toEqual({stageTokens:[]});
    await expect(d.call("ingest_publication_scoped_claim_v1",{...conn,stageToken:d.stage})).rejects.toThrow("admission busy");
    await expect(d.call("ingest_publication_scoped_publish_v1",{binding,stageToken:d.stage,attemptToken:ticket.attemptToken,leaseToken:ticket.leaseToken,messages:[]})).rejects.toMatchObject({code:"40001"});
    expect((await d.pool.query("SELECT consumed,attempt FROM sessions.fixture_scoped_admission")).rows).toEqual([{consumed:false,attempt:ticket.attemptToken}]);
  },30000);

  test("an independently committed privacy change invalidates a blocked scoped claim",async()=>{
    const {d,conn}=await scopedFixture();
    const control=await d.pool.connect(),claimer=await d.pool.connect();
    let result:Promise<{code:string,message:string}|null>|undefined;
    try{
      await control.query("BEGIN");
      await control.query("SELECT sessions.ingest_lock_fences($1,$2,$3)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
      await control.query("UPDATE sessions.ingest_buffer_admission SET privacy_baseline_sha256=repeat('d',64) WHERE org_id=$1",[TENANT.orgId]);
      await claimer.query("SET ROLE bulls96_session_ingest");
      const pid=(await claimer.query("SELECT pg_backend_pid() pid")).rows[0].pid;
      result=claimer.query("SELECT sessions.ingest_publication_scoped_claim_v1($1)",[JSON.stringify({...conn,stageToken:d.stage})]).then(()=>null,e=>({code:e.code,message:e.message}));
      const deadline=Date.now()+3000;
      let blocked=false;
      do {
        blocked=(await control.query("SELECT pg_backend_pid()=ANY(pg_blocking_pids($1)) blocked",[pid])).rows[0].blocked;
        if(!blocked) await Bun.sleep(10);
      } while(!blocked&&Date.now()<deadline);
      expect(blocked).toBe(true);
      await control.query("COMMIT");
      expect(await result).toMatchObject({message:"scoped fixture: authority stale"});
      expect((await d.pool.query("SELECT count(*)::int n FROM sessions.ingest_publication_attempts")).rows[0].n).toBe(0);
    } finally {
      await control.query("ROLLBACK"); await result;
      control.release();claimer.release(true);
    }
  },30000);

});

describe.skipIf(!url)("post-H scoped kernel proposal", () => {
  const runtime = "bulls96_publication_scoped", recovery = "bulls96_publication_recovery";

  async function setup(store?: ObjectStore) {
    const d = await fixture(true,undefined,false,store);
    const login = "scope_" + crypto.randomUUID().replaceAll("-", "");
    // The fixture URL is an explicitly disposable cluster. Refuse existing roles.
    if (!orderedTemplate) await d.pool.query("CREATE ROLE "+runtime+" NOLOGIN; CREATE ROLE "+recovery+" NOLOGIN");
    await d.pool.query("CREATE ROLE "+login+" LOGIN NOINHERIT");
    clean.push(async()=>{
      for(const role of (orderedTemplate ? [login] : [login,runtime,recovery])) {
        await d.pool.query("DROP OWNED BY "+role); await d.pool.query("DROP ROLE "+role);
      }
    });
    await d.pool.query("GRANT "+runtime+" TO "+login+"; GRANT USAGE ON SCHEMA sessions TO "+runtime+","+recovery);
    // Only these held slots enter this disposable rollback ledger. The fixture
    // supplies synthetic admissions and does not stand in for a 062270 issuer.
    await d.pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text NOT NULL)");
    for (const slot of [publicationKernelSlot, publicationRecoverySlot]) {
      if (orderedTemplate) {
        const row=await d.pool.query("SELECT sha256 FROM sessions.__96bulls_migrations WHERE name=$1",[slot.name]);
        expect(row.rows).toEqual([{sha256:slot.sha256}]);
        expect((await d.pool.query(heldPublicationSentinels[slot.name].sql)).rows[0].applied).toBe(true);
        continue;
      }
      const c = await d.pool.connect();
      try {
        const sentinel=heldPublicationSentinels[slot.name].sql;
        expect((await c.query(sentinel)).rows[0].applied).toBe(false);
        await c.query("BEGIN"); await c.query(slot.up);
        expect((await c.query(sentinel)).rows[0].applied).toBe(true);
        await c.query("INSERT INTO sessions.__96bulls_migrations(name,sha256) VALUES($1,$2)",[slot.name,slot.sha256]);
        await c.query("COMMIT");
      } finally { await c.query("ROLLBACK"); c.release(); }
    }
    const binding = {planSha256:"c".repeat(64), admissionId:CANARY_PLAN_ID,frontierSha256:"e".repeat(64),
      tenant:{...TENANT,originId:"origin-1"},target:d.target};
    const admit = () => d.pool.query(`INSERT INTO publication_private.admission(id,binding,stage,frontier_image,admission_image,stage_sha,enabled,valid_until)
      SELECT $1,$2,s.stage_token,to_jsonb(f),to_jsonb(a),s.stage_sha256,true,clock_timestamp()+interval '5 minutes' FROM sessions.ingest_stage_intents s
      JOIN sessions.ingest_publication_frontiers f USING(org_id,workspace_id,user_id,origin_id)
      JOIN sessions.ingest_buffer_admission a USING(org_id) WHERE s.stage_token=$3`,[binding.admissionId,binding,d.stage]);
    const db=(await d.pool.query("SELECT current_database() AS db")).rows[0].db;
    const u=new URL(url!);u.pathname="/"+db;u.username=login;u.password="";
    const connect=async()=>{
      const c=new pg.Client({connectionString:u.href});await c.connect();await c.query("SET ROLE "+runtime);return c;
    };
    const addr={generation:GEN,binding,bucket:d.target.bucket,region:d.target.region,keyRoot:d.target.keyRoot};
    const invoke=async(c: {query:Function}, op:string, p:object) =>
      (await c.query("SELECT sessions.ingest_publication_scoped_"+op+"_v1($1) AS result",[p])).rows[0].result;
    return {d,binding,admit,connect,addr,invoke};
  }

  test("isolated LOGIN can claim and heartbeat; each mutation refuses foreign bindings and ordinary calls remain available to ingest",async()=>{
    const {d,connect,invoke,admit,addr,binding}=await setup(); await admit();
    const c=await connect();
    try {
      for(const role of [recovery,"bulls96_session_ingest"]) await expect(c.query("SET ROLE "+role)).rejects.toMatchObject({code:"42501"});
      await expect(c.query("SELECT * FROM publication_private.admission")).rejects.toMatchObject({code:"42501"});
      await expect(c.query("SELECT publication_private.claim('{}')")).rejects.toMatchObject({code:"42501"});
      for(const op of ["candidates","claim","heartbeat","prepare","register_objects","publish","abandon"]) {
        await expect(c.query("SELECT sessions.ingest_publication_"+op+"('{}')")).rejects.toMatchObject({code:"42501"});
      }
      expect((await d.call("ingest_publication_candidates",{...d.target,provider:undefined,limit:1})).stageTokens).toEqual([d.stage]);
      expect(await invoke(c,"candidates",{...addr,limit:1})).toEqual({stageTokens:[d.stage]});
      await d.call("ingest_publication_defer",{stageToken:d.stage,code:"40001"});
      expect(await invoke(c,"candidates",{...addr,limit:1})).toEqual({stageTokens:[]});
      await expect(invoke(c,"claim",{...addr,stageToken:d.stage})).rejects.toThrow("deferred stage");
      await d.pool.query("UPDATE sessions.ingest_publication_deferrals SET retry_after=clock_timestamp() WHERE stage_token=$1",[d.stage]);
      const t=await invoke(c,"claim",{...addr,stageToken:d.stage});
      const p={generation:GEN,binding,stageToken:d.stage,attemptToken:t.attemptToken,leaseToken:t.leaseToken};
      expect(await invoke(c,"heartbeat",p)).toHaveProperty("leaseUntil");
      for(const [op,extra] of [["heartbeat",{}],["prepare",{images:[]}],["register_objects",{objects:[]}],["publish",{messages:[]}],["abandon",{}]] as const) {
        await expect(invoke(c,op,{...p,...extra,binding:{...binding,planSha256:"a".repeat(64)}})).rejects.toThrow("binding mismatch");
      }
      await expect(invoke(c,"recover",p)).rejects.toMatchObject({code:"42501"});
      expect(await invoke(c,"abandon",p)).toEqual({abandoned:true});
      await expect(invoke(c,"candidates",{...addr,limit:1})).rejects.toThrow("authority stale");
    } finally {await c.end();}
  },30000);

  test("the dedicated role completes a canonical publication and replays only its exact final request",async()=>{
    const {d,connect,invoke,admit,addr,binding}=await setup(); await admit();
    const c=await connect();
    const pin=(t:PublicationTicket)=>({stageToken:t.stageToken,attemptToken:t.attemptToken,leaseToken:t.leaseToken,generation:GEN,binding});
    // Exercise SQL independently of the separately packaged host adapter.
    const rpc:PublicationRpc={
      candidates:async limit=>(await invoke(c,"candidates",{...addr,limit})).stageTokens,
      claim:async stageToken=>parsePublicationTicket(await invoke(c,"claim",{...addr,stageToken}),d.target),
      heartbeat:async t=>{t.leaseUntil=(await invoke(c,"heartbeat",pin(t))).leaseUntil;},
      register:async(t,objects)=>{expect(await invoke(c,"register_objects",{...pin(t),objects})).toEqual({registered:true});},
      prepare:async(t,images)=>{expect((await invoke(c,"prepare",{...pin(t),images})).prepared).toBe(true);},
      publish:async(t,messages)=>{
        const request={...pin(t),messages};
        const result=await invoke(c,"publish",request);
        expect(result).toMatchObject({published:true,replay:false});
        expect(await invoke(c,"publish",request)).toEqual({...result,replay:true});
        await expect(invoke(c,"publish",{...request,messages:[]})).rejects.toMatchObject({code:"40001"});
        return result;
      },
      abandon:async t=>{await invoke(c,"abandon",pin(t));},
      defer:async()=>{throw new Error("unexpected defer");},
    };
    try {
      expect(await canonicalPublicationTick({rpc,target:d.target,store:()=>d.store},{enabled:true,candidateLimit:1})).toMatchObject({published:1});
      expect((await d.pool.query("SELECT state FROM sessions.ingest_publication_attempts")).rows).toEqual([{state:"published"}]);
    } finally {await c.end();}
  },30000);

  test("the scoped host replays a lost final acknowledgement across dedicated connections",async()=>{
    const {scopedPublicationRpc}=await import("../publication-scoped-rpc.ts");
    const store=new VersionedMemoryStore();
    const {d,binding,admit,connect}=await setup(store as unknown as ObjectStore);
    await admit();
    let client=await connect();
    const rpc=scopedPublicationRpc({queryBounded:async(q:string,params:unknown[])=>client.query(q,params)} as never,d.target,binding);
    let replays=0;
    const watched={...rpc,publish:async(...args:Parameters<typeof rpc.publish>)=>{
      const first=await rpc.publish(...args);
      expect(first.replay).toBe(false);
      const versions = store.versions().map(({key,id})=>({key,id}));
      expect(versions.length).toBeGreaterThan(1);
      // Discard the acknowledged connection. Retry on a fresh dedicated login.
      await client.end();client=await connect();
      const second=await rpc.publish(...args);
      expect(second).toEqual({...first,replay:true});
      await expect(rpc.publish(args[0],[],args[2])).rejects.toMatchObject({code:"40001"});
      expect(store.versions().map(({key,id})=>({key,id}))).toEqual(versions);
      replays++;
      return second;
    }};
    try {
      expect(await canonicalPublicationTick({rpc:watched,target:d.target,store:()=>d.store},{enabled:true,candidateLimit:1})).toMatchObject({published:1});
      expect(replays).toBe(1);
      expect((await d.pool.query("SELECT state FROM sessions.ingest_publication_attempts")).rows).toEqual([{state:"published"}]);
    } finally {await client.end();}
  },30000);

  test("operator may recover exactly three expired claims; replay and stale tickets cannot supersede a newer attempt",async()=>{
    const {d,connect,invoke,admit,addr,binding}=await setup(); await admit();
    const operator=await d.pool.connect(); await operator.query("SET ROLE "+recovery);
    const c=await connect();
    try {
      let previous: object | undefined;
      for(let i=0;i<4;i++){
        const claimant=await connect();
        let ticket;
        try { ticket=await invoke(claimant,"claim",{...addr,stageToken:d.stage}); } finally { await claimant.end(); }
        const p={generation:GEN,stageToken:d.stage,binding,attemptToken:ticket.attemptToken,leaseToken:ticket.leaseToken};
        await expect(invoke(operator,"recover",p)).rejects.toThrow(i===3?"recovery not admitted":"recovery lease or stage changed");
        await d.pool.query("UPDATE sessions.ingest_publication_attempts SET lease_until=clock_timestamp()-interval '1s' WHERE attempt_token=$1",[ticket.attemptToken]);
        await d.pool.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1s' WHERE stage_token=$1",[d.stage]);
        await d.pool.query("UPDATE sessions.ingest_publication_heads SET build_lease_until=clock_timestamp()-interval '1s' WHERE build_attempt_token=$1",[ticket.attemptToken]);
        if(previous){
          expect(await invoke(operator,"recover",previous)).toEqual({recovered:true,replay:true});
          expect((await d.pool.query("SELECT attempt FROM publication_private.admission")).rows[0].attempt).toBe(ticket.attemptToken);
        }
        if(i===3){
          await expect(invoke(operator,"recover",p)).rejects.toThrow("recovery not admitted");
          expect(await invoke(c,"candidates",{...addr,limit:1})).toEqual({stageTokens:[]});
          break;
        }
        expect(await invoke(operator,"recover",p)).toEqual({recovered:true,replay:false});
        expect(await invoke(operator,"recover",p)).toEqual({recovered:true,replay:true});
        await expect(invoke(c,"heartbeat",p)).rejects.toThrow("attempt mismatch");
        expect(await invoke(c,"candidates",{...addr,limit:1})).toEqual({stageTokens:[d.stage]});
        previous=p;
      }
      expect((await d.pool.query("SELECT count(*)::int n FROM publication_private.expired_tickets")).rows[0].n).toBe(3);
      if (orderedTemplate) await d.pool.query(read(searchDown));
      const rollback=await d.pool.connect();
      try {
        await expect(rollback.query(publicationRecoverySlot.down)).rejects.toThrow("retain recovery receipts");
        await rollback.query("ROLLBACK");
        expect((await rollback.query("SELECT count(*)::int n FROM publication_private.expired_tickets")).rows[0].n).toBe(3);
      } finally {rollback.release(true);}
    } finally {await c.end();await operator.query("RESET ROLE");operator.release();}
  },30000);

  test("a privacy change commits ahead of blocked recovery and leaves the claim and receipts intact",async()=>{
    const {d,connect,invoke,admit,addr,binding}=await setup(); await admit();
    const c=await connect(), operator=await d.pool.connect(), control=await d.pool.connect();
    let pending:Promise<unknown>|undefined;
    try {
      const t=await invoke(c,"claim",{...addr,stageToken:d.stage});
      const p={generation:GEN,stageToken:d.stage,binding,attemptToken:t.attemptToken,leaseToken:t.leaseToken};
      await d.pool.query("UPDATE sessions.ingest_publication_attempts SET lease_until=clock_timestamp()-interval '1s' WHERE attempt_token=$1",[t.attemptToken]);
      await d.pool.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1s' WHERE stage_token=$1",[d.stage]);
      await operator.query("SET ROLE "+recovery);
      const pid=(await operator.query("SELECT pg_backend_pid() pid")).rows[0].pid;
      await control.query("BEGIN");
      await control.query("SELECT sessions.ingest_lock_fences($1,$2,$3)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
      await control.query("UPDATE sessions.ingest_buffer_admission SET privacy_baseline_sha256=repeat('d',64)");
      pending=invoke(operator,"recover",p).then(()=>null,e=>e.message);
      const deadline=Date.now()+3000;
      let blocked=false;
      do {
        blocked=(await control.query("SELECT pg_backend_pid()=ANY(pg_blocking_pids($1)) blocked",[pid])).rows[0].blocked;
        if(!blocked) await Bun.sleep(10);
      } while(!blocked&&Date.now()<deadline);
      expect(blocked).toBe(true);
      await control.query("COMMIT");
      expect(await pending).toBe("scoped publication: authority stale");
      expect((await d.pool.query("SELECT attempts_recovered,attempt FROM publication_private.admission")).rows[0]).toEqual({attempts_recovered:0,attempt:t.attemptToken});
      expect((await d.pool.query("SELECT count(*)::int n FROM publication_private.expired_tickets")).rows[0].n).toBe(0);
    } finally {
      await control.query("ROLLBACK");await pending;await c.end();
      await operator.query("RESET ROLE");operator.release();control.release();
    }
  },30000);

  test("empty-state rollback removes new APIs and leaves ordinary ingest grants intact",async()=>{
    const {d}=await setup();
    if (orderedTemplate) await d.pool.query(read(searchDown));
    const c=await d.pool.connect();
    try {
      await expect(c.query(publicationKernelSlot.down)).rejects.toMatchObject({code:"55000"});
      await c.query("ROLLBACK");
      await c.query("UPDATE sessions.__96bulls_migrations SET sha256=repeat('0',64) WHERE name=$1",[publicationRecoverySlot.name]);
      await expect(c.query(publicationRecoverySlot.down)).rejects.toMatchObject({code:"55000"});
      await c.query("ROLLBACK");
      await c.query("UPDATE sessions.__96bulls_migrations SET sha256=$2 WHERE name=$1",[publicationRecoverySlot.name,publicationRecoverySlot.sha256]);
    } finally {c.release();}
    await d.pool.query(publicationRecoverySlot.down);
    await d.pool.query(publicationKernelSlot.down);
    for (const {sql} of Object.values(heldPublicationSentinels)) expect((await d.pool.query(sql)).rows[0].applied).toBe(false);
    expect((await d.pool.query("SELECT to_regnamespace('publication_private') gone")).rows[0].gone).toBeNull();
    expect((await d.call("ingest_publication_candidates",{...d.target,provider:undefined,limit:1})).stageTokens).toEqual([d.stage]);
  },30000);
});

describe.skipIf(!url)("062270 private read profile",()=>{
  test("two authenticated logins cannot reuse one another's profile; closing read observes committed revocation",async()=>{
    const d=await fixture();
    const reader="bulls96_frontier_receipt_reader";
    const suffix=crypto.randomUUID().replaceAll("-","");
    const issuerLogin="reader_"+suffix, secondLogin="other_"+suffix;
    await d.pool.query("CREATE ROLE "+reader+" NOLOGIN NOINHERIT; CREATE ROLE "+issuerLogin+" LOGIN NOINHERIT; CREATE ROLE "+secondLogin+" LOGIN NOINHERIT");
    clean.push(async()=>{
      for(const role of [issuerLogin,secondLogin,reader]){
        await d.pool.query("DROP OWNED BY "+role);await d.pool.query("DROP ROLE "+role);
      }
    });
    await d.pool.query("GRANT "+reader+" TO "+issuerLogin+","+secondLogin+"; GRANT USAGE ON SCHEMA sessions TO "+reader);
    const proposal="supabase/proposals/continuous-publication/062270-private-read-profile";
    if (!orderedTemplate) await d.pool.query(read(proposal+".up.sql"));
    await d.pool.query(read(proposal+".down.sql"));
    expect((await d.pool.query("SELECT to_regnamespace('frontier_receipt_private') AS gone")).rows[0].gone).toBeNull();
    await d.pool.query(read(proposal+".up.sql"));
    // This fixture wrapper exposes only a Boolean. It cannot enumerate receipts.
    await d.pool.query(`CREATE FUNCTION sessions.profile_gate_probe(p jsonb) RETURNS boolean
      LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS
      $$ SELECT (frontier_receipt_private.check_request(p)).profile_id IS NOT NULL $$;
      REVOKE ALL ON FUNCTION sessions.profile_gate_probe(jsonb) FROM PUBLIC;
      GRANT EXECUTE ON FUNCTION sessions.profile_gate_probe(jsonb) TO `+reader);
    const tenant={...TENANT,originId:"origin-1"};
    const request={protocol:"first-ack-seat-capture-v1",profileId:crypto.randomUUID(),profileSha256:"c".repeat(64),tenant,
      generation:GEN,after:null,limit:2};
    const authority={sourceIdentitySha256:"1".repeat(64),sourceGenerationSha256:"2".repeat(64),sourceHead:"3".repeat(40),
      controlEpoch:"4".repeat(64),privacyBaselineSha256:"5".repeat(64),privacyVersion:"0"};
    await d.pool.query(`INSERT INTO frontier_receipt_private.read_profiles
      SELECT $1,$2,r.oid,r.rolname,$3,$4,$5,$6,clock_timestamp(),clock_timestamp()+interval '5 minutes',2,100,32768
      FROM pg_roles r WHERE r.rolname=$7`,[request.profileId,request.profileSha256,tenant,GEN,d.target,authority,issuerLogin]);
    const db=(await d.pool.query("SELECT current_database() db")).rows[0].db;
    const connect=async(login:string)=>{
      const u=new URL(url!);u.pathname="/"+db;u.username=login;u.password="";
      const c=new pg.Client({connectionString:u.href});await c.connect();await c.query("SET ROLE "+reader);return c;
    };
    const first=await connect(issuerLogin),other=await connect(secondLogin);
    const select=(c:pg.Client,p:object)=>c.query("SELECT sessions.profile_gate_probe($1) ok",[p]);
    const begin=(c:pg.Client)=>c.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    async function refuse(c:pg.Client,p:object){
      await begin(c);
      try {await expect(select(c,p)).rejects.toMatchObject({code:"42501"});}
      finally{await c.query("ROLLBACK");}
    }
    try {
      await expect(select(first,request)).rejects.toMatchObject({code:"42501"});
      await expect(first.query("SELECT * FROM frontier_receipt_private.read_profiles")).rejects.toMatchObject({code:"42501"});
      await refuse(other,request);
      await refuse(first,{...request,tenant:{...tenant,userId:"another"}});
      await refuse(first,{...request,profileSha256:"a".repeat(64)});
      await refuse(first,{...request,limit:3});
      await begin(first);
      expect((await select(first,request)).rows[0].ok).toBe(true);
      await d.pool.query("INSERT INTO frontier_receipt_private.revoked_profiles(profile_id) VALUES($1)",[request.profileId]);
      await first.query("COMMIT");
      await refuse(first,request);
      await expect(d.pool.query("UPDATE frontier_receipt_private.read_profiles SET page_limit=1000")).rejects.toThrow("append-only");
      expect((await d.pool.query("SELECT to_regprocedure('sessions.ingest_scoped_publication_receipt_capture_v1(jsonb)') AS capture")).rows[0].capture).toBeNull();
      const teardown=await d.pool.connect();
      try {
        await expect(teardown.query(read(proposal+".down.sql"))).rejects.toThrow("retain enrolled profiles");
        await teardown.query("ROLLBACK");
        expect((await teardown.query("SELECT count(*)::int n FROM frontier_receipt_private.revoked_profiles")).rows[0].n).toBe(1);
      } finally {teardown.release(true);}
    } finally {await first.end();await other.end();}
  },30000);
});

describe.skipIf(!url)("062270 independent synthetic receipt inventory",()=>{
  test("requires enrollment, both sides of all receipt classes and unchanged evidence on the closing pass",async()=>{
    const {captureFrontierReceiptSource,RECEIPT_CAPTURE_IDENTITY_SQL}=await import("../frontier-receipt-source.ts");
    const d=await fixture(),reader="bulls96_frontier_receipt_reader",suffix=crypto.randomUUID().replaceAll("-","");
    const login="reader_"+suffix,issuer="inventory_"+suffix,destination="dest_"+suffix;
    await d.pool.query("CREATE ROLE "+reader+" NOLOGIN NOINHERIT; CREATE ROLE "+login+" LOGIN NOINHERIT; CREATE ROLE "+issuer+" NOLOGIN; CREATE ROLE "+destination+" NOLOGIN");
    clean.push(async()=>{for(const role of [login,reader,issuer,destination]){
      await d.pool.query("DROP OWNED BY "+role);await d.pool.query("DROP ROLE "+role);
    }});
    await d.pool.query("GRANT "+reader+" TO "+login+"; GRANT USAGE ON SCHEMA sessions TO "+reader);
    await d.pool.query(read("supabase/proposals/continuous-publication/062270-private-read-profile.up.sql"));
    await d.pool.query(read("src/session-ingest/__tests__/fixtures/frontier-independent-receipt-domain.sql"));
    await d.pool.query("GRANT USAGE ON SCHEMA fixture_receipt_inventory TO "+issuer+","+destination+
      "; GRANT INSERT,SELECT ON fixture_receipt_inventory.enrollment,fixture_receipt_inventory.expected TO "+issuer+
      "; GRANT INSERT,SELECT ON fixture_receipt_inventory.observed TO "+destination);
    const profileId=crypto.randomUUID(),profileSha256="d".repeat(64);
    const tenant={orgId:"org-a",workspaceId:"ws-a",userId:"user-a",originId:"origin-1"};
    const authority={sourceIdentitySha256:"1".repeat(64),sourceGenerationSha256:"2".repeat(64),sourceHead:"3".repeat(40),
      controlEpoch:"4".repeat(64),privacyBaselineSha256:"5".repeat(64),privacyVersion:"9"};
    await d.pool.query(`INSERT INTO frontier_receipt_private.read_profiles SELECT
      $1,$2,r.oid,r.rolname,$3,$4,$5,$6,clock_timestamp(),clock_timestamp()+interval '5 minutes',2,32,16384
      FROM pg_roles r WHERE r.rolname=$7`,[profileId,profileSha256,tenant,GEN,d.target,authority,login]);
    const db=(await d.pool.query("SELECT current_database() db")).rows[0].db;
    const u=new URL(url!);u.pathname="/"+db;u.username=login;u.password="";
    const client=new pg.Client({connectionString:u.href});await client.connect();
    const source=await d.pool.connect(),target=await d.pool.connect();
    try{
      await source.query("SET ROLE "+issuer);await target.query("SET ROLE "+destination);await client.query("SET ROLE "+reader);
      await expect(source.query("SELECT * FROM fixture_receipt_inventory.observed")).rejects.toMatchObject({code:"42501"});
      await expect(target.query("SELECT * FROM fixture_receipt_inventory.expected")).rejects.toMatchObject({code:"42501"});
      await expect(client.query("SELECT * FROM fixture_receipt_inventory.enrollment")).rejects.toMatchObject({code:"42501"});
      // Independently authored terminal observations, deliberately delivered out of order.
      await target.query(`INSERT INTO fixture_receipt_inventory.observed
        SELECT $1,source,ack,jsonb_build_object('tenant',$2::jsonb,'recordId',record,'revision',rev,'revisionId',rid,
          'source','fixture','sessionId',NULL,'seat',seat,'bodyAccepted',accept,'candidateDigest',repeat(body,64),'firstDigest',repeat(first,64)),true
        FROM (VALUES ('queue','D','r3','2','v4','23',true,'d','d'),
          ('native','A','r1','7','v1','21',true,'a','a'),('legacy','C','r2','1','v3','22',true,'c','c'),
          ('s3_first','E','r4','3','v5','24',true,'e','e'),('equal_revision_loser','B','r1','7','v2','21',false,'b','a')
        ) raw(source,ack,record,rev,rid,seat,accept,body,first)`,[profileId,tenant]);
      // Catalog extraction is only the disposable fixture's pin; real pins must be reviewed separately.
      const identity=(await d.pool.query(RECEIPT_CAPTURE_IDENTITY_SQL)).rows[0];
      const input={tenant,generation:GEN,profileId,profileSha256,functionOwner:identity.function_owner,functionSha256:identity.definition_sha256,
        bounds:{pageLimit:2,maxPages:8,maxDecisions:32,maxBytes:64000,maxDurationMs:10000}};
      const capture=()=>captureFrontierReceiptSource(client as never,input);
      const assertHeld=async(reason:string)=>{
        await expect(capture()).rejects.toThrow("frontier_receipt_unavailable");
        await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
        try {
          await expect(client.query("SELECT sessions.ingest_scoped_publication_receipt_capture_v1($1)",[
            {protocol:"first-ack-seat-capture-v1",profileId,profileSha256,tenant,generation:GEN,after:null,limit:2},
          ])).rejects.toThrow(reason);
        } finally {await client.query("ROLLBACK");}
      };
      await assertHeld("enrollment held");
      // Fixed independent known answer. No observed destination row is read to issue the enrollment.
      const root="38a3d36d1f50f2380a4def8f89c6cc847a176f5e4205217bac973c0feb34cedd";
      const counts={native:1,equal_revision_loser:1,legacy:1,queue:1,s3_first:1};
      await source.query("INSERT INTO fixture_receipt_inventory.enrollment VALUES ($1,$2,$3,$4,$5,$6,$7)",
        [profileId,tenant,GEN,d.target,authority,counts,root]);
      const manifest=[
        ["native","A","r1","7","v1","21","a","a"],["equal_revision_loser","B","r1","7","v2","21","b","a"],
        ["legacy","C","r2","1","v3","22","c","c"],["queue","D","r3","2","v4","23","d","d"],["s3_first","E","r4","3","v5","24","e","e"],
      ];
      for(const [population,ack,recordId,revision,revisionId,seat,body,first] of manifest){
        await source.query("INSERT INTO fixture_receipt_inventory.expected VALUES ($1,$2,$3,$4)",
          [profileId,population,ack,{tenant,recordId,revision,revisionId,seat,source:"fixture",sessionId:null,
            bodyAccepted:body===first,candidateDigest:body.repeat(64),firstDigest:first.repeat(64)}]);
      }
      const complete=await capture();
      expect(complete.scan.pageItemCounts).toEqual([2,2,1,0]);
      expect(complete.decisions.map(x=>[x.revisionId,x.bodyAccepted])).toEqual([["v1",true],["v2",false],["v3",true],["v4",true],["v5",true]]);
      for(const population of Object.keys(counts)){
        const lost=(await d.pool.query("DELETE FROM fixture_receipt_inventory.observed WHERE profile_id=$1 AND source=$2 RETURNING *",[profileId,population])).rows[0];
        await assertHeld("terminal comparison held");
        await target.query("INSERT INTO fixture_receipt_inventory.observed VALUES ($1,$2,$3,$4,$5)",
          [profileId,population,lost.ack_id,lost.decision,lost.terminal]);
      }
      let commits=0;
      const interrupted={query:async(q:string,args?:unknown[])=>{
        const result=await client.query(q,args);
        if(q==="COMMIT" && ++commits===1){
          await d.pool.query("UPDATE fixture_receipt_inventory.observed SET terminal=false WHERE source='equal_revision_loser'");
        }
        return result;
      }};
      await expect(captureFrontierReceiptSource(interrupted as never,input)).rejects.toThrow("frontier_receipt_unavailable");
      expect(commits).toBe(1);
      await d.pool.query("UPDATE fixture_receipt_inventory.observed SET terminal=true WHERE source='equal_revision_loser'");
      // Same number of entries on both sides cannot rescue a forged decision.
      await d.pool.query("UPDATE fixture_receipt_inventory.observed SET decision=jsonb_set(decision,'{seat}','\"99\"') WHERE source='queue'");
      await assertHeld("terminal comparison held");
      await d.pool.query("UPDATE fixture_receipt_inventory.observed SET decision=jsonb_set(decision,'{seat}','\"23\"') WHERE source='queue'");
      // Even simultaneous corruption of both row stores is detected by the immutable test vector.
      for(const table of ["expected","observed"]){
        await d.pool.query("UPDATE fixture_receipt_inventory."+table+" SET decision=jsonb_set(decision,'{seat}','\"98\"') WHERE source='queue'");
      }
      await assertHeld("independent root mismatch");
      for(const table of ["expected","observed"]){
        await d.pool.query("UPDATE fixture_receipt_inventory."+table+" SET decision=jsonb_set(decision,'{seat}','\"23\"') WHERE source='queue'");
      }
      // A missing cohort on BOTH sides still disagrees with the issuer's independent terminal count.
      await d.pool.query("DELETE FROM fixture_receipt_inventory.expected WHERE source='legacy'");
      await d.pool.query("DELETE FROM fixture_receipt_inventory.observed WHERE source='legacy'");
      await assertHeld("terminal comparison held");
    } finally {
      await client.end();await source.query("RESET ROLE");await target.query("RESET ROLE");source.release();target.release();
    }
  },30000);
});
