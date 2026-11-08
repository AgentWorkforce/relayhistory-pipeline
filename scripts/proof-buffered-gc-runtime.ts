import {orderedFixtureLedger,initializeFixtureSeatClock} from "../src/session-ingest/__tests__/ordered-support.ts";
/** Actual owned Node24 -> PG17 -> producer/typed GC ports -> versioned MinIO.
 * No production URL/credential is accepted. Synthetic localhost containers only.
 */
import pg from "pg";
import {execFileSync} from "node:child_process";
import {mkdirSync,readFileSync,writeFileSync} from "node:fs";
import {join} from "node:path";
import {createHash} from "node:crypto";
import {zstdDecompressSync} from "node:zlib";
import {runMigrations,generation} from "../src/hoopdb/migrate.ts";
import {createS3Store,objectKey,readAll,ObjectStoreError,type ObjectStore} from "../src/shared/vendor.ts";
import {signRequest} from "../packages/object-store/src/sigv4.ts";
import {sha256Hex} from "../packages/object-store/src/crypto.ts";
import {xmlElements,xmlText} from "../packages/object-store/src/xml.ts";
import {createS3Eraser} from "../src/migration/neon-s3-erasure/eraser.ts";
import {sqlBufferedGcLedger,collectBufferedTicket,bufferedGcTick,type BufferedGcLedger} from "../src/session-ingest/buffered-gc.ts";
import {sqlLedger} from "../src/session-ingest/ledger-sql.ts";
import {deliverBatch} from "../src/session-ingest/deliver.ts";
import {readBufferedStage,BUFFER_STAGE_MAX_BYTES,type PreparedBufferedStage} from "../src/session-ingest/staging.ts";
import {deliveryAccount} from "../src/session-ingest/vendor/delivery/prepare.ts";
import {DELIVERY_MAPPING_VERSION,type HistoryExportBatch} from "../src/session-ingest/vendor/delivery/delivery-contracts.ts";
import {runBufferedGcJob as sourceJob} from "../src/session-ingest/buffered-gc-job.ts";
import {SemanticPostgresExecutor} from "../src/search-features/semantic-copy-postgres.ts";
const out=process.env.GC_RUNTIME_PROOF_OUT??"";
const jobBundle=process.env.GC_RUNTIME_JOB_BUNDLE;
if(jobBundle&&!/^\/home\/khaliqgant\/relay-briefs\/gc-runtime-build-[A-Za-z0-9_-]+\/bundle\/index\.mjs$/.test(jobBundle))throw Error("owned_bundle_only");
const runBufferedGcJob:typeof sourceJob=jobBundle?(await import(jobBundle)).runBufferedGcJob:sourceJob;
if(process.env.GC_RUNTIME_DISPOSABLE!=="1"||!out.startsWith("/home/khaliqgant/relay-briefs/gc-runtime-proof-"))throw Error("disposable_only");
function port(name:string,key:string){
 const d=JSON.parse(execFileSync("docker",["inspect",name],{encoding:"utf8"}))[0];
 if(d.Config.Labels["com.96bulls.proof"]!=="buffer-gc-runtime")throw Error("container_owner");
 const p=d.NetworkSettings.Ports[key][0];if(p.HostIp!=="127.0.0.1")throw Error("loopback_only");
 return Number(p.HostPort);
}
const fixturePrefix=process.env.GC_RUNTIME_FIXTURE_PREFIX??"buffer-gc-runtime";
if(!["buffer-gc-runtime","root-buffer-gc-capacity"].includes(fixturePrefix))throw Error("fixture_prefix_refused");
const pgPort=port(fixturePrefix+"-pg17-20261104","5432/tcp"),s3Port=port(fixturePrefix+"-minio-20261104","9000/tcp");
const head=execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim();
const dirty=!!execFileSync("git",["status","--porcelain"],{encoding:"utf8"}).trim();
if(dirty&&!process.argv.includes("--allow-wip"))throw Error("source_dirty");
mkdirSync(out,{recursive:false,mode:0o700});
const proof:any={head,dirty,node:process.version,cases:[],jobBundle:jobBundle??null,failed:0,requests:0,intentBeforePut:0};
const credentials={accessKeyId:"gc-fixture-access",secretAccessKey:"gc-fixture-only-key-20261104"};
const endpoint="http://127.0.0.1:"+s3Port,bucket="gc-runtime-fixture",region="us-east-1";
const hash=(s:string)=>createHash("sha256").update(s).digest("hex");
const assert=(v:unknown,code:string)=>{if(!v)throw Error(code)};
const cfg=(database:string,user="postgres")=>({host:"127.0.0.1",port:pgPort,user,database,connectionTimeoutMillis:1000,application_name:"gc-runtime-proof-"+user});
const admin=new pg.Client(cfg("postgres"));await admin.connect();
await admin.query("DROP DATABASE IF EXISTS buffer_gc_runtime_fixture WITH(FORCE)");
await admin.query("CREATE DATABASE buffer_gc_runtime_fixture");await admin.end();
const owner=new pg.Client(cfg("buffer_gc_runtime_fixture"));owner.on("error",()=>{});await owner.connect();
let pool:pg.Pool|undefined,wrong:pg.Pool|undefined,stage="setup";
const fetcher=(async(...args:Parameters<typeof fetch>)=>{proof.requests++;return fetch(...args)}) as typeof fetch;
const store=createS3Store({endpoint,bucket,region,credentials,fetch:fetcher});
const eraser=createS3Eraser({endpoint,bucket,region,credentials,fetch:fetcher});
async function s3(method:string,suffix="",body=new Uint8Array()){
 const url=new URL(endpoint+"/"+bucket+suffix),headers=new Headers({"content-type":"application/xml"});
 await signRequest({method,url,headers,payloadSha256:await sha256Hex(body),region,credentials});
 const r=await fetcher(url,{method,headers,...(body.byteLength?{body}:{}),signal:AbortSignal.timeout(5000)});
 if(!r.ok)throw Error("fixture_s3_"+r.status);return r;
}
async function versions(key:string){
 const r=await s3("GET","?versions=&prefix="+encodeURIComponent(key));
 const xml=await r.text(),rows=[];
 assert(xmlText(xml,"IsTruncated")==="false","fixture_version_inventory");
 for(const tag of ["Version","DeleteMarker"])for(const e of xmlElements(xml,tag))
  if(xmlText(e,"Key")===key)rows.push({id:xmlText(e,"VersionId"),latest:xmlText(e,"IsLatest")==="true",marker:tag==="DeleteMarker"});
 return rows;
}
async function check(name:string,work:()=>Promise<void>){stage=name;const t=Date.now();await work();proof.cases.push({name,ms:Date.now()-t,pass:true})}
async function rejected(p:Promise<unknown>,code:string){const e=await p.then(()=>null,e=>e);assert(e?.code===code||e?.message===code,"expected_"+code)}
function proxied(original:ObjectStore,put:(...a:any[])=>Promise<any>):ObjectStore{return new Proxy(original,{get(target,key){if(key==="put")return put;const v=(target as any)[key];return typeof v==="function"?v.bind(target):v}})}
const unwrap=(b:Uint8Array)=>new Uint8Array(zstdDecompressSync(b,{maxOutputLength:BUFFER_STAGE_MAX_BYTES}));
let serial=0;
try{
 await runMigrations(owner,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}});
 const expected=await generation();proof.generation=expected;
 await owner.query(readFileSync(join(process.cwd(),"supabase/bootstrap/session_ingest_role.sql"),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n"));
 await owner.query(readFileSync(join(process.cwd(),"supabase/bootstrap/session_ingest_verify.sql"),"utf8"));
 await s3("HEAD").catch(async e=>{if(e.message!=="fixture_s3_404")throw e;await s3("PUT")});
 await s3("PUT","?versioning",new TextEncoder().encode('<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>'));
 assert((await (await s3("GET","?versioning")).text()).includes("<Status>Enabled</Status>"),"versioning_enabled");
 pool=new pg.Pool({...cfg("buffer_gc_runtime_fixture","bulls96_session_ingest"),max:2});pool.on("error",()=>{});
 const executor=new SemanticPostgresExecutor(pool);
 const run=async(sql:string,params:unknown[])=>executor.queryBounded<Record<string,unknown>>(sql,params,{signal:AbortSignal.timeout(5000),deadline:Date.now()+5000});
 const producer=orderedFixtureLedger(sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:expected}),owner);
 const gc=sqlBufferedGcLedger({query:run},expected);
 async function fixture(){
  const n=++serial,tenant={orgId:"gc-org-"+n,workspaceId:"ws-"+n,userId:"user-"+n},origin="origin-"+n;
  await owner.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
  try{await owner.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)",[JSON.stringify({org_id:tenant.orgId,workspace_id:tenant.workspaceId,user_id:tenant.userId,deletions:[],resets:[]})]);await owner.query("COMMIT")}
  catch(e){await owner.query("ROLLBACK");throw e}
  await owner.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',repeat('3',64))",[tenant.orgId,expected]);
  await initializeFixtureSeatClock(owner,expected);
  const records=["gone","kept"].map((id,i)=>({schema_version:1 as const,origin_id:origin,revision_id:"actual-revision-"+id,record_id:"record-"+id,revision:7,kind:"session_event" as const,source:"claude",session_id:id,operation:"upsert" as const,payload:{text:i?"keep exact legitimate message":"erase this original message",ts_ms:1761955200000}}));
  const batch:HistoryExportBatch={schema_version:1,origin_id:origin,batch_id:"actual-batch-"+n,job_id:"job-"+n,generation:1,destination_id:"fixture",instance_id:"fixture",account_id:await deliveryAccount(tenant),mapping_version:DELIVERY_MAPPING_VERSION,records};
  const row=async()=>{const r=(await owner.query("SELECT * FROM sessions.ingest_stage_intents WHERE org_id=$1 AND parent_stage_token IS NULL ORDER BY created_at DESC LIMIT 1",[tenant.orgId])).rows[0];assert(r,"intent");return r};
  const ticket=async()=>{const r=await gc.claim(100);const t=r.find(x=>x.stageToken!==undefined&&x.key.startsWith("96bulls/"+tenant.orgId+"/"));assert(t,"gc_ticket");return t!};
  const disable=()=>producer.setSessionUploads({tenant,originId:origin,sessions:[["claude","gone"]],enabled:false});
  const deliver=(s:ObjectStore=store)=>deliverBatch({ledger:producer,acceptanceMode:"buffered",target:{store:s,keyRoot:"96bulls",sleep:async()=>{}}},tenant,batch);
  return {tenant,origin,batch,row,ticket,disable,deliver};
 }
 const auditStore=proxied(store,async(key,body,opts)=>{
  const r=(await owner.query("SELECT cleanup_state FROM sessions.ingest_stage_intents WHERE stage_key=$1",[String(key)])).rows[0];
  assert(r,"intent_must_precede_put");proof.intentBeforePut++;
  return store.put(key,body,opts);
 });
 await check("actual buffered ACK mixed batch -> GC keeps current group and permanently removes all old versions",async()=>{
  const f=await fixture(),accepted=await f.deliver();assert(accepted.outcome==="accepted_buffered","actual_ack");
  assert(accepted.receipt.acceptedRevisionIds.join(",")==="actual-revision-gone,actual-revision-kept","original_receipt_ids");
  const old=await f.row(),raw=await readAll((await store.getObject(objectKey(old.stage_key))).body);
  await store.put(objectKey(old.stage_key),raw);await store.delete(objectKey(old.stage_key));await store.put(objectKey(old.stage_key),raw);
  assert((await versions(old.stage_key)).length>=4,"multiple_old_versions");
  await f.disable();const ticket=await f.ticket();assert(ticket.keepGroupIndexes.join(",")==="1","kept_group");
  assert(await collectBufferedTicket(ticket,{ledger:gc,store:auditStore,eraser,keyRoot:"96bulls"})==="finished","gc_finished");
  const current=(await owner.query("SELECT t.* FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents t ON t.stage_token=o.active_stage_token WHERE o.org_id=$1",[f.tenant.orgId])).rows[0];
  assert(current.stage_token!==old.stage_token&&current.receipt_state==="acknowledged","atomic_current_child");
  const decoded=await readBufferedStage(store,{key:current.stage_key,sha256:current.stage_sha256,bytes:Number(current.stage_bytes),groupOrdinals:current.group_ordinals},unwrap);
  assert(decoded.groups.length===1&&decoded.groups[0]!.sessionId==="kept","only_surviving_group");
  assert(decoded.groups[0]!.lines[0]!.includes("keep exact legitimate message")&&!decoded.groups[0]!.lines[0]!.includes("erase this"),"kept_public_bytes");
  const remaining=await versions(old.stage_key);assert(remaining.length===1&&!remaining[0]!.marker&&remaining[0]!.latest,"barrier_only");
  await rejected(store.put(objectKey(old.stage_key),raw,{ifNoneMatch:true}),"already_exists");
 });
 await check("preparing not-yet-PUT expiry installs a barrier, refuses late bytes and never ACKs",async()=>{
  const f=await fixture();let pending:{key:any;body:Uint8Array;opts:any}|undefined;
  const stopped=proxied(store,async(key,body,opts)=>{pending={key,body:new Uint8Array(body),opts};throw Error("proof_before_put")});
  await f.deliver(stopped).catch(()=>{});assert(pending,"captured_before_put");
  const old=await f.row();assert(old.receipt_state==="preparing","unacknowledged");
  await owner.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[old.stage_token]);
  const t=await f.ticket();assert(t.phase==="cleanup"&&t.receiptState==="preparing","abort_without_ack");
  const missing403=new Proxy(auditStore,{get(target,key){const v=(target as any)[key];if(key==="getObject")return async(...args:any[])=>{try{return await v.apply(target,args)}catch(e){if((e as any).code==="not_found")throw new ObjectStoreError("unauthorized","s3","missing object intentionally hidden",{status:403});throw e}};return typeof v==="function"?v.bind(target):v}});
  assert(await collectBufferedTicket(t,{ledger:gc,store:missing403,eraser,keyRoot:"96bulls"})==="finished","aborted_cleanup");
  await rejected(store.put(pending!.key,pending!.body,pending!.opts),"already_exists");
  await producer.commitBuffered!({tenant:f.tenant,originId:f.origin,batchId:f.batch.batch_id,batchDigest:old.digest,claimToken:old.claim_token,stageToken:old.stage_token}).then(()=>{throw Error("late_ack")},()=>{});
  assert((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===0,"no_receipt");
  assert((await versions(old.stage_key)).length===1,"one_abort_barrier");
 });
 await check("actual SQL CAS false leaves the current object untouched",async()=>{
  const f=await fixture();await f.deliver();const old=await f.row();await f.disable();const t=await f.ticket();
  const before=await versions(old.stage_key);let erased=0;
  const changed:BufferedGcLedger={...gc,async swap(ticket,child){
   await owner.query("UPDATE sessions.ingest_buffer_outbox SET row_version=row_version+1 WHERE active_stage_token=$1",[old.stage_token]);
   return gc.swap(ticket,child);
  }};
  const result=await collectBufferedTicket(t,{ledger:changed,store:auditStore,eraser:{async eraseObjectExcept(...args){erased++;return eraser.eraseObjectExcept(...args)}},keyRoot:"96bulls"});
  assert(result==="deferred"&&erased===0,"no_erase_after_failed_swap");
  assert(JSON.stringify(await versions(old.stage_key))===JSON.stringify(before),"current_versions_untouched");
  assert((await owner.query("SELECT active_stage_token FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].active_stage_token===old.stage_token,"current_pointer_untouched");
 });
 await check("crash after swap and overlapping cleanup workers converge on one persistent barrier",async()=>{
  const f=await fixture();await f.deliver();const old=await f.row();await f.disable();const t=await f.ticket();
  const crashing:BufferedGcLedger={...gc,async swap(ticket,child){assert(await gc.swap(ticket,child),"real_swap");throw Error("crash_after_swap")}};
  await collectBufferedTicket(t,{ledger:crashing,store:auditStore,eraser,keyRoot:"96bulls"}).then(()=>{throw Error("crash_missing")},()=>{});
  assert((await owner.query("SELECT cleanup_state FROM sessions.ingest_stage_intents WHERE stage_token=$1",[old.stage_token])).rows[0].cleanup_state==="retired","durable_retirement");
  await owner.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[old.stage_token]);
  const cleanup=await f.ticket();assert(cleanup.phase==="cleanup","resume_no_original_raw");
  await store.delete(objectKey(old.stage_key)); // Current GET missing; prior versions remain.
  const results=await Promise.all([collectBufferedTicket(cleanup,{ledger:gc,store:auditStore,eraser,keyRoot:"96bulls"}),collectBufferedTicket(cleanup,{ledger:gc,store:auditStore,eraser,keyRoot:"96bulls"})]);
  assert(results.every(x=>x==="finished"),"overlapping_cleanup");
  const v=await versions(old.stage_key);assert(v.length===1&&v[0]!.latest&&!v[0]!.marker,"same_barrier_preserved");
  const data=await readAll((await store.getObject(objectKey(old.stage_key))).body);assert(new TextDecoder().decode(data).includes("buffered-redaction"),"persistent_barrier_body");
 });
 await check("two crashes after replacement PUT retain the same child and recover after both leases expire",async()=>{
  const f=await fixture();await f.deliver();const old=await f.row();await f.disable();
  let firstChild:string|undefined,firstKey:string|undefined;
  for(let attempt=0;attempt<2;attempt++){
   const t=await f.ticket();let child:string|undefined;
   const crash:BufferedGcLedger={...gc,async prepare(ticket,spec){child=await gc.prepare(ticket,spec);return child},async swap(){throw Error("crash_before_swap")}};
   await collectBufferedTicket(t,{ledger:crash,store:auditStore,eraser,keyRoot:"96bulls"}).then(()=>{throw Error("no_crash")},e=>assert(e.message==="crash_before_swap","crash_stage"));
   const row=(await owner.query("SELECT * FROM sessions.ingest_stage_intents WHERE stage_token=$1",[child])).rows[0];
   if(!firstChild){firstChild=child;firstKey=row.stage_key}else assert(child===firstChild&&row.stage_key===firstKey,"deterministic_child_preserved");
   assert(row.cleanup_state==="live"&&row.receipt_state==="preparing","child_not_retired");
   await owner.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()-interval '1 second' WHERE stage_token=$1",[old.stage_token]);
   await owner.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()-interval '1 second',gc_checked_at=clock_timestamp()-interval '1 hour' WHERE stage_token=$1",[child]);
  }
  const t=await f.ticket();assert(t.replacement?.stageToken===firstChild,"same_registered_child");
  assert(await collectBufferedTicket(t,{ledger:gc,store:auditStore,eraser,keyRoot:"96bulls"})==="finished","double_crash_recovered");
  const current=(await owner.query("SELECT active_stage_token FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0];
  assert(current.active_stage_token===firstChild,"child_is_current");
  const v=await versions(firstKey!);assert(v.some(x=>x.latest&&!x.marker),"live_child_not_redacted");
  const body=await readAll((await store.getObject(objectKey(firstKey!))).body);assert(body.byteLength>0&&!new TextDecoder().decode(body).includes("buffered-redaction"),"kept_stage_intact");
 });
 await check("failed PUT resumes the same generation and returns the original durable receipt",async()=>{
  const f=await fixture(),stopped=proxied(store,async()=>{throw Error("owned_put_failure")});
  await f.deliver(stopped).then(()=>{throw Error("failed_put_ack")},()=>{});
  const first=await f.row();assert(first.receipt_state==="preparing","failed_put_preparing");
  const retry=await f.deliver();assert(retry.outcome==="accepted_buffered","same_generation_retry");
  assert(retry.receipt.acceptedRevisionIds.join(",")==="actual-revision-gone,actual-revision-kept","same_original_ids");
  const replay=await f.deliver();assert(replay.outcome==="replay","durable_retry_replay");
  assert((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===1,"one_outbox");
 });
 await check("pending or changed generation refuses producer ACK and GC metadata, including old nonce reuse",async()=>{
  const f=await fixture(),before=proof.requests;
  await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pending'");
  await f.deliver().then(()=>{throw Error("pending_ack")},()=>{});
  await gc.claim(1).then(()=>{throw Error("pending_gc")},()=>{});
  assert(proof.requests===before,"pending_before_s3");
  await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pass'");
  const flip=proxied(store,async(key,body,opts)=>{const r=await store.put(key,body,opts);await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pending'");return r});
  await f.deliver(flip).then(()=>{throw Error("post_put_pending_ack")},()=>{});
  const old=await f.row();assert(old.receipt_state==="preparing","pending_no_ack");
  const next="f".repeat(64);await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pass',generation=$1",[next]);
  await owner.query("UPDATE sessions.ingest_buffer_admission SET schema_generation=$1 WHERE org_id=$2",[next,f.tenant.orgId]);
  const newer=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:next});
  await newer.commitBuffered!({tenant:f.tenant,originId:f.origin,batchId:f.batch.batch_id,batchDigest:old.digest,claimToken:old.claim_token,stageToken:old.stage_token}).then(()=>{throw Error("borrowed_new_generation")},e=>assert(e.code==="ledger_sqlstate_40001","original_claim_generation_guard"));
  assert((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===0,"old_nonce_not_ack");
  await owner.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status='pass'",[expected]);
 });
 await check("admission expiry after a blocked ACK write rolls back receipt and outbox atomically",async()=>{
  const f=await fixture();
  await owner.query("CREATE FUNCTION public.gc_proof_pause_outbox() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_sleep(0.25);RETURN NEW;END$$;CREATE TRIGGER gc_proof_pause_outbox BEFORE INSERT ON sessions.ingest_buffer_outbox FOR EACH ROW EXECUTE FUNCTION public.gc_proof_pause_outbox()");
  try{
   const expire=proxied(store,async(key,body,opts)=>{const result=await store.put(key,body,opts);await owner.query("UPDATE sessions.ingest_buffer_admission SET valid_until=clock_timestamp()+interval '0.15 second' WHERE org_id=$1",[f.tenant.orgId]);return result});
   await f.deliver(expire).then(()=>{throw Error("expired_after_write_ack")},()=>{});
   assert((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===0,"expired_write_rollback_outbox");
   assert((await f.row()).receipt_state==="preparing","expired_no_ack");
   assert((await owner.query("SELECT state FROM sessions.ingest_batches WHERE org_id=$1",[f.tenant.orgId])).rows[0].state==="pending","expired_write_rollback_receipt");
  }finally{await owner.query("DROP TRIGGER gc_proof_pause_outbox ON sessions.ingest_buffer_outbox;DROP FUNCTION public.gc_proof_pause_outbox()")}
 });
 for(const control of ["selection","reset"] as const)await check("admission close waits in-flight "+control+" and rejects subsequent mutations before writes",async()=>{
  const f=await fixture(),blocker=new pg.Client({...cfg("buffer_gc_runtime_fixture"),application_name:"gc-runtime-user-blocker"}),closer=new pg.Client({...cfg("buffer_gc_runtime_fixture"),application_name:"gc-runtime-admission-closer"});
  await blocker.connect();await closer.connect();
  try{
   await blocker.query("BEGIN");await blocker.query("SELECT 1 FROM sessions.ingest_fence_owners WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3 FOR UPDATE",[f.tenant.orgId,f.tenant.workspaceId,f.tenant.userId]);
   const pending=control==="selection"?producer.setSessionUploads({tenant:f.tenant,originId:f.origin,sessions:[["claude","gone"]],enabled:false}):producer.resetUploadedSessions(f.tenant,"in-flight-reset");
   let blocked=false;for(let i=0;i<100;i++){blocked=(await owner.query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='gc-runtime-proof-bulls96_session_ingest' AND wait_event_type='Lock') AS yes")).rows[0].yes;if(blocked)break;await new Promise(r=>setTimeout(r,5))}
   assert(blocked,"control_waits_user");
   const close=closer.query("UPDATE sessions.ingest_buffer_admission SET valid_until=clock_timestamp()-interval '1 second' WHERE org_id=$1",[f.tenant.orgId]);
   let waiting=false;for(let i=0;i<100;i++){waiting=(await owner.query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='gc-runtime-admission-closer' AND wait_event_type='Lock') AS yes")).rows[0].yes;if(waiting)break;await new Promise(r=>setTimeout(r,5))}
   assert(waiting,"close_waits_control_share");
   await blocker.query("COMMIT");await pending;await close;
   const before=(await owner.query("SELECT (SELECT count(*)::int FROM sessions.upload_resets WHERE org_id=$1) AS r,(SELECT count(*)::int FROM sessions.upload_deletions WHERE org_id=$1) AS d",( [f.tenant.orgId]))).rows[0],requests=proof.requests;
   await f.deliver().then(()=>{throw Error("closed_ack")},()=>{});
   await producer.setSessionUploads({tenant:f.tenant,originId:f.origin,sessions:[["claude","gone"]],enabled:true}).then(()=>{throw Error("closed_selection")},()=>{});
   await producer.resetUploadedSessions(f.tenant,"after-close").then(()=>{throw Error("closed_reset")},()=>{});
   // An omitted mode cannot become canonical publication for an enrolled org.
   await deliverBatch({ledger:sqlLedger(run),target:{store,keyRoot:"96bulls",sleep:async()=>{}}},f.tenant,f.batch).then(()=>{throw Error("missing_mode_wrote")},()=>{});
   assert(proof.requests===requests,"closed_before_object_io");
   const after=(await owner.query("SELECT (SELECT count(*)::int FROM sessions.upload_resets WHERE org_id=$1) AS r,(SELECT count(*)::int FROM sessions.upload_deletions WHERE org_id=$1) AS d",[f.tenant.orgId])).rows[0];
   assert(JSON.stringify(before)===JSON.stringify(after),"closed_control_no_writes");
   assert((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_batches WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===0,"missing_mode_no_batch");
  }finally{await blocker.query("ROLLBACK").catch(()=>{});await blocker.end();await closer.end()}
 });
 await check("permanent job uses compiled four-verifier assets and actual bounded PG/MinIO transports",async()=>{
  const f=await fixture();await f.deliver();const old=await f.row();await f.disable();
  const saved={AWS_ACCESS_KEY_ID:process.env.AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY:process.env.AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN:process.env.AWS_SESSION_TOKEN};
  process.env.AWS_ACCESS_KEY_ID="ASIA"+"X".repeat(16);process.env.AWS_SECRET_ACCESS_KEY="synthetic-only";process.env.AWS_SESSION_TOKEN="synthetic-execution-role-token";
  try{
   let poolChecked=false,storeChecked=false;
   const result=await runBufferedGcJob({AWS_REGION:"us-east-1",HOOPDB_DB_MODE:"supabase",INGEST_GC_ENABLED:"true",INGEST_GC_BUCKET:bucket,
    INGEST_GC_AWS_ACCOUNT_ID:"111122223333",INGEST_GC_SUPABASE_PROJECT_REF:"abcdefghijklmnopqrst",
    INGEST_GC_KMS_KEY_ARN:"arn:aws:kms:us-east-1:111122223333:key/11111111-1111-4111-8111-111111111111",
    INGEST_GC_SECRET_ARN:"arn:aws:secretsmanager:us-east-1:111122223333:secret:96bulls/proof/buffered-gc-abcdef",INGEST_GC_EXPECT_GENERATION:expected,INGEST_GC_BATCH_LIMIT:"20"},{
    secret:async()=>JSON.stringify({hoopdb_url:"postgresql://bulls96_session_ingest.abcdefghijklmnopqrst:synthetic-unused@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"}),
    pool:c=>{assert(c.host==="aws-0-us-east-1.pooler.supabase.com"&&c.port===5432&&(c.ssl as any).rejectUnauthorized===true,"job_pinned_tls");poolChecked=true;return new pg.Pool({...cfg("buffer_gc_runtime_fixture","bulls96_session_ingest"),max:2})},
    store:c=>{assert(c.serverSideEncryption==="aws:kms"&&c.sseKmsKeyId?.includes("111122223333"),"job_sse_kms");storeChecked=true;return auditStore},
    eraser:()=>eraser
   });
   assert(result.finished>=1&&poolChecked&&storeChecked,"job_real_finished");
   const oldState=(await owner.query("SELECT cleanup_state FROM sessions.ingest_stage_intents WHERE stage_token=$1",[old.stage_token])).rows[0];
   assert(oldState.cleanup_state==="redacted","job_sql_finished");
  }finally{for(const[k,v]of Object.entries(saved)){if(v===undefined)delete process.env[k];else process.env[k]=v}}
 });
 await check("permanent job cancels its blocked metadata query, closes its pool and leaves the unrelated blocker alive",async()=>{
  const lock=new pg.Client({...cfg("buffer_gc_runtime_fixture"),application_name:"gc-job-owned-blocker"});await lock.connect();
  const saved={AWS_ACCESS_KEY_ID:process.env.AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY:process.env.AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN:process.env.AWS_SESSION_TOKEN};
  process.env.AWS_ACCESS_KEY_ID="ASIA"+"X".repeat(16);process.env.AWS_SECRET_ACCESS_KEY="synthetic-only";process.env.AWS_SESSION_TOKEN="synthetic-execution-role-token";
  try{
   await lock.query("BEGIN;UPDATE sessions.__96bulls_role_validation SET status='pass'");
   const start=Date.now(),before=proof.requests;
   const error=await runBufferedGcJob({AWS_REGION:"us-east-1",HOOPDB_DB_MODE:"supabase",INGEST_GC_ENABLED:"true",INGEST_GC_BUCKET:bucket,
    INGEST_GC_AWS_ACCOUNT_ID:"111122223333",INGEST_GC_SUPABASE_PROJECT_REF:"abcdefghijklmnopqrst",
    INGEST_GC_KMS_KEY_ARN:"arn:aws:kms:us-east-1:111122223333:key/11111111-1111-4111-8111-111111111111",
    INGEST_GC_SECRET_ARN:"arn:aws:secretsmanager:us-east-1:111122223333:secret:96bulls/proof/buffered-gc-abcdef",INGEST_GC_EXPECT_GENERATION:expected},{
    secret:async()=>JSON.stringify({hoopdb_url:"postgresql://bulls96_session_ingest.abcdefghijklmnopqrst:synthetic-unused@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"}),
    pool:()=>new pg.Pool({...cfg("buffer_gc_runtime_fixture","bulls96_session_ingest"),application_name:"gc-job-bounded",max:2}),
    store:()=>auditStore,eraser:()=>eraser
   }).then(()=>null,e=>e);
   assert(error&&["sql_55P03","sql_57014"].includes(error.code),"bounded_lock_refusal");
   assert(Date.now()-start<3500,"bounded_job_return");
   assert(proof.requests===before,"blocked_job_no_object_io");
   assert((await lock.query("SELECT 1 AS n")).rows[0].n===1,"unrelated_blocker_alive");
   const active=(await owner.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name='gc-job-bounded'")).rows[0].n;
   assert(active===0,"no_owned_active_or_idle_backend_after_return");
  }finally{await lock.query("ROLLBACK").catch(()=>{});await lock.end();for(const[k,v]of Object.entries(saved)){if(v===undefined)delete process.env[k];else process.env[k]=v}}
 });
 await check("wrong database role refuses through actual typed SQL ports before object access",async()=>{
  await owner.query("DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='gc_runtime_readonly') THEN CREATE ROLE gc_runtime_readonly LOGIN NOINHERIT;END IF;END$$;GRANT USAGE ON SCHEMA sessions TO gc_runtime_readonly");
  wrong=new pg.Pool({...cfg("buffer_gc_runtime_fixture","gc_runtime_readonly"),max:1});wrong.on("error",()=>{});
  const x=new SemanticPostgresExecutor(wrong),before=proof.requests;
  const bad=sqlBufferedGcLedger({query:(s,p)=>x.query(s,p)},expected);
  await bad.claim(1).then(()=>{throw Error("wrong_role_accepted")},e=>assert(e.code==="sql_42501","wrong_role_code"));
  assert(proof.requests===before,"wrong_role_no_io");
 });
 await check("default permanent claim handles twenty maximum legal500-record stages within its1.8s metadata budget",async()=>{
  const orgs:string[]=[];
  for(let b=0;b<20;b++){
   const f=await fixture();orgs.push(f.tenant.orgId);
   f.batch.records=Array.from({length:500},(_,i)=>({schema_version:1 as const,origin_id:f.origin,record_id:"cap-record-"+i,revision_id:"cap-revision-"+i,revision:1,kind:"session_event" as const,source:"claude",session_id:"cap-session-"+i,operation:"upsert" as const,payload:{text:"synthetic capacity message "+i,ts_ms:1761955200000}}));
   const ack=await f.deliver();assert(ack.outcome==="accepted_buffered","capacity_producer_ack");
  }
  await owner.query("UPDATE sessions.ingest_stage_intents SET gc_checked_at=clock_timestamp()+interval '1 day' WHERE NOT(org_id=ANY($1::text[]))",[orgs]);
  const bounded=sqlBufferedGcLedger({query:(s,p)=>executor.queryBounded(s,p,{signal:AbortSignal.timeout(1800),deadline:Date.now()+1800})},expected);
  const start=Date.now(),tickets=await bounded.claim(5),ms=Date.now()-start;
  assert(tickets.length===0,"healthy_capacity_no_cleanup");
  const seen=(await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_stage_intents WHERE org_id=ANY($1::text[]) AND gc_checked_at IS NOT NULL",[orgs])).rows[0].n;
  assert(seen===20,"all_twenty_maximum_candidates_scanned");assert(ms<1800,"maximum_shape_claim_budget");
  proof.capacity={batches:20,recordsPerBatch:500,totalRecords:10000,claimLimit:5,candidatesScanned:seen,elapsedMs:ms};
 });
 proof.passed=proof.cases.length;proof.maxRSSBytes=process.resourceUsage().maxRSS*1024;
}catch(e){proof.failed=1;proof.error={stage,code:(e as any)?.code??(e as any)?.message};process.exitCode=1}
finally{await Promise.allSettled([owner.end(),...(pool?[pool.end()]:[]),...(wrong?[wrong.end()]:[])]);writeFileSync(join(out,"proof.json"),JSON.stringify(proof,null,2)+"\n");console.log(JSON.stringify(proof))}
