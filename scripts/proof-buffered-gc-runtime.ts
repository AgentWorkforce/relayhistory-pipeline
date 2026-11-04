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
import {createS3Store,objectKey,readAll,type ObjectStore} from "../src/shared/vendor.ts";
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
import {SemanticPostgresExecutor} from "../src/search-features/semantic-copy-postgres.ts";
const out=process.env.GC_RUNTIME_PROOF_OUT??"";
if(process.env.GC_RUNTIME_DISPOSABLE!=="1"||!out.startsWith("/home/khaliqgant/relay-briefs/gc-runtime-proof-"))throw Error("disposable_only");
function port(name:string,key:string){
 const d=JSON.parse(execFileSync("docker",["inspect",name],{encoding:"utf8"}))[0];
 if(d.Config.Labels["com.96bulls.proof"]!=="buffer-gc-runtime")throw Error("container_owner");
 const p=d.NetworkSettings.Ports[key][0];if(p.HostIp!=="127.0.0.1")throw Error("loopback_only");
 return Number(p.HostPort);
}
const pgPort=port("buffer-gc-runtime-pg17-20261104","5432/tcp"),s3Port=port("buffer-gc-runtime-minio-20261104","9000/tcp");
const head=execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim();
const dirty=!!execFileSync("git",["status","--porcelain"],{encoding:"utf8"}).trim();
if(dirty&&!process.argv.includes("--allow-wip"))throw Error("source_dirty");
mkdirSync(out,{recursive:false,mode:0o700});
const proof:any={head,dirty,node:process.version,cases:[],failed:0,requests:0,intentBeforePut:0};
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
 const producer=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:expected});
 const gc=sqlBufferedGcLedger({query:run},expected);
 async function fixture(){
  const n=++serial,tenant={orgId:"gc-org-"+n,workspaceId:"ws-"+n,userId:"user-"+n},origin="origin-"+n;
  await owner.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
  try{await owner.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)",[JSON.stringify({org_id:tenant.orgId,workspace_id:tenant.workspaceId,user_id:tenant.userId,deletions:[],resets:[]})]);await owner.query("COMMIT")}
  catch(e){await owner.query("ROLLBACK");throw e}
  await owner.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,repeat('a',64),repeat('b',64),clock_timestamp()+interval '1 hour')",[tenant.orgId]);
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
  assert(await collectBufferedTicket(t,{ledger:gc,store:auditStore,eraser,keyRoot:"96bulls"})==="finished","aborted_cleanup");
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
  const newer=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:next});
  await newer.commitBuffered!({tenant:f.tenant,originId:f.origin,batchId:f.batch.batch_id,batchDigest:old.digest,claimToken:old.claim_token,stageToken:old.stage_token}).then(()=>{throw Error("borrowed_new_generation")},()=>{});
  assert((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===0,"old_nonce_not_ack");
  await owner.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status='pass'",[expected]);
 });
 await check("wrong database role refuses through actual typed SQL ports before object access",async()=>{
  await owner.query("DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='gc_runtime_readonly') THEN CREATE ROLE gc_runtime_readonly LOGIN NOINHERIT;END IF;END$$;GRANT USAGE ON SCHEMA sessions TO gc_runtime_readonly");
  wrong=new pg.Pool({...cfg("buffer_gc_runtime_fixture","gc_runtime_readonly"),max:1});wrong.on("error",()=>{});
  const x=new SemanticPostgresExecutor(wrong),before=proof.requests;
  const bad=sqlBufferedGcLedger({query:(s,p)=>x.query(s,p)},expected);
  await bad.claim(1).then(()=>{throw Error("wrong_role_accepted")},e=>assert(e.code==="sql_42501","wrong_role_code"));
  assert(proof.requests===before,"wrong_role_no_io");
 });
 proof.passed=proof.cases.length;proof.maxRSSBytes=process.resourceUsage().maxRSS*1024;
}catch(e){proof.failed=1;proof.error={stage,code:(e as any)?.code??(e as any)?.message};process.exitCode=1}
finally{await Promise.allSettled([owner.end(),...(pool?[pool.end()]:[]),...(wrong?[wrong.end()]:[])]);writeFileSync(join(out,"proof.json"),JSON.stringify(proof,null,2)+"\n");console.log(JSON.stringify(proof))}
