import {initializeFixtureSeatClock} from "../src/session-ingest/__tests__/ordered-support.ts";
/** Owned PG17 target proof; no production URL/credentials or inferred epoch. */
import pg from "pg";
import {createHash} from "node:crypto";
import {execFileSync} from "node:child_process";
import {mkdirSync,writeFileSync,readFileSync} from "node:fs";
import {runMigrations,generation} from "../src/hoopdb/migrate.ts";
import {SemanticPostgresExecutor} from "../src/search-features/semantic-copy-postgres.ts";
import {sqlLedger} from "../src/session-ingest/ledger-sql.ts";
import {createIngestApp,encodeIdentity,signRequest,IDENTITY_HEADER,SIGNATURE_HEADER,SYNC_SCOPE} from "../src/session-ingest/app.ts";
import {deliveryAccount} from "../src/session-ingest/vendor/delivery/prepare.ts";
import {DELIVERY_MAPPING_VERSION} from "../src/session-ingest/vendor/delivery/delivery-contracts.ts";
import type {OrderedControl} from "../src/session-ingest/control-protocol.ts";
import type {SelectionInput,LedgerTenant} from "../src/session-ingest/ledger.ts";
import { initScrubEngine } from "../packages/pipeline/src/lib/scrub-engine/index.ts";
// Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
await initScrubEngine();
const out=process.env.ORDERED_CONTROL_PROOF_OUT??"";
if(process.env.ORDERED_CONTROL_DISPOSABLE!=="1"||!out.startsWith("/home/khaliqgant/relay-briefs/ordered-controls-proof-"))throw Error("owned_only");
const inspect=JSON.parse(execFileSync("docker",["inspect","ordered-controls-pg17-20261104"],{encoding:"utf8"}))[0];
if(inspect.Config.Labels["com.96bulls.proof"]!=="ordered-controls-runtime")throw Error("owned_container");
const published=inspect.NetworkSettings.Ports["5432/tcp"][0];if(published.HostIp!=="127.0.0.1")throw Error("loopback_only");
const cfg=(database:string,user="postgres")=>({host:"127.0.0.1",port:Number(published.HostPort),database,user,connectionTimeoutMillis:1000,application_name:"ordered-controls-proof"});
const head=execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim(),dirty=!!execFileSync("git",["status","--porcelain"],{encoding:"utf8"}).trim();
if(dirty&&!process.argv.includes("--allow-wip"))throw Error("source_dirty");
mkdirSync(out,{recursive:false,mode:0o700});const proof:any={head,dirty,node:process.version,cases:[],sourceOracle:"target PG17 exact tuple; actual Neon journal integration pending"};
const admin=new pg.Client(cfg("postgres"));await admin.connect();await admin.query("DROP DATABASE IF EXISTS ordered_controls_fixture WITH(FORCE)");await admin.query("CREATE DATABASE ordered_controls_fixture");await admin.end();
const owner=new pg.Client(cfg("ordered_controls_fixture"));owner.on("error",()=>{});await owner.connect();
const pool=new pg.Pool({...cfg("ordered_controls_fixture","bulls96_session_ingest"),max:4});pool.on("error",()=>{});
const x=new SemanticPostgresExecutor(pool),run=(s:string,p:unknown[])=>x.queryBounded<Record<string,unknown>>(s,p,{signal:AbortSignal.timeout(5000),deadline:Date.now()+5000});
const yes=(v:unknown,s:string)=>{if(!v)throw Error(s)};
const expected=await generation(),epoch="3".repeat(64);
let stage="setup",serial=0;let sourceOwner:pg.Client|undefined;
async function check(name:string,f:()=>Promise<void>){if(process.env.ORDERED_CONTROL_CASE&&!name.includes(process.env.ORDERED_CONTROL_CASE))return;stage=name;const at=Date.now();await f();proof.cases.push({name,ms:Date.now()-at,pass:true})}
async function refuses(p:Promise<unknown>){const e=await p.then(()=>null,e=>e);yes(e,"expected_refusal");return e}
try{
 await runMigrations(owner,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}});
 await owner.query(readFileSync(new URL("../supabase/bootstrap/session_ingest_role.sql",import.meta.url),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n"));
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:expected});proof.generation=expected;
 async function fixture(){
  const n=++serial,tenant={orgId:"ordered-org-"+n,workspaceId:"ws-"+n,userId:"user-"+n},origin="origin-"+n;
  await owner.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
  try{await owner.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)",[JSON.stringify({org_id:tenant.orgId,workspace_id:tenant.workspaceId,user_id:tenant.userId,deletions:[],resets:[]})]);await owner.query("COMMIT")}catch(e){await owner.query("ROLLBACK");throw e}
  await owner.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',$3)",[tenant.orgId,expected,epoch]);
  await initializeFixtureSeatClock(owner,expected);
  const selection=(enabled:boolean):SelectionInput=>({tenant,originId:origin,sessions:[["claude","session"]],enabled});
  const control=async(kind:OrderedControl["kind"],version:string,payload:unknown,e=epoch):Promise<OrderedControl>=>{
   const r=await owner.query("SELECT encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,$5::text,$6::jsonb)::text,'UTF8')),'hex') AS h",[e,tenant.orgId,tenant.workspaceId,tenant.userId,kind,JSON.stringify(payload)]);
   return{epoch:e,version,digest:r.rows[0].h,kind};
  };
  const select=async(version:string,enabled:boolean,e=epoch)=>{const input=selection(enabled),c=await control("selection",version,{origin_id:origin,sessions:[{source:"claude",session_id:"session"}],enabled},e);return{input,control:{...c,kind:"selection" as const},apply:()=>ledger.applyOrderedControl!({...input,control:{...c,kind:"selection"}})}};
  const reset=async(version:string,requestId:string)=>{const c=await control("reset",version,{request_id:requestId});return{control:{...c,kind:"reset" as const},apply:()=>ledger.applyOrderedControl!({tenant,requestId,control:{...c,kind:"reset"}})}};
  const state=async()=>JSON.stringify((await owner.query("SELECT (SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY source,session_id),'[]') FROM sessions.upload_deletions d WHERE org_id=$1) AS deletions,(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY request_id),'[]') FROM sessions.upload_resets r WHERE org_id=$1) AS resets,(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY version),'[]') FROM sessions.ingest_control_receipts c WHERE org_id=$1) AS controls,(SELECT coalesce(jsonb_agg(to_jsonb(v) ORDER BY source,session_id),'[]') FROM sessions.ingest_fence_versions v WHERE org_id=$1) AS versions",[tenant.orgId])).rows[0]);
  const deleted=async()=>(await owner.query("SELECT count(*)::int AS n FROM sessions.upload_deletions WHERE org_id=$1",[tenant.orgId])).rows[0].n as number;
  return{tenant,origin,selection,select,reset,state,deleted};
 }
 for(const [older,newer]of [[false,true],[true,false]])await check("late "+(older?"enable":"disable")+" cannot overwrite newer "+(newer?"enable":"disable"),async()=>{
  const f=await fixture(),a=await f.select("1",older),b=await f.select("2",newer);
  yes((await a.apply()).control.outcome==="applied","v1_apply");yes((await b.apply()).control.outcome==="applied","v2_apply");
  const before=await f.state(),r=await a.apply();yes(r.control.outcome==="stale"&&r.control.version==="1"&&r.control.digest===a.control.digest,"exact_stale_ack");
  yes(await f.state()===before,"stale_no_mutation");yes(await f.deleted()===(newer?0:1),"newer_selection_preserved");
 });
 await check("reset late replay after reenable preserves latest fences and original deleted count",async()=>{
  const f=await fixture();await(await f.select("1",true)).apply();const reset=await f.reset("2","reset-original");const first=await reset.apply();await(await f.select("3",true)).apply();
  const before=await f.state(),again=await reset.apply();yes(again.control.outcome==="stale"&&again.deleted===first.deleted,"reset_exact_ack");yes(await f.state()===before&&await f.deleted()===0,"reset_no_late_tombstone");
 });
 await check("foreign epoch, gap, conflicting historical digest and malformed decimal versions refuse without state change",async()=>{
  const f=await fixture(),before=await f.state();
  await refuses((await f.select("1",false,"4".repeat(64))).apply());await refuses((await f.select("2",false)).apply());yes(await f.state()===before,"foreign_gap_no_state");
  const one=await f.select("1",false);await one.apply();await(await f.select("2",true)).apply();
  const current=await f.state();await refuses((await f.select("1",true)).apply());
  for(const version of ["0","01","9223372036854775808"])await refuses(ledger.applyOrderedControl!({...one.input,control:{...one.control,version}}));
  yes(await f.state()===current,"conflicts_no_state");
 });
 await check("positive BIGINT versions greater than 2^53 remain exact through SQL and adapter",async()=>{
  const f=await fixture();await owner.query("INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation) VALUES($1,$2,$3,$4,9007199254740992,repeat('a',64),'selection','{\"updated\":1}'::jsonb,$5)",[f.tenant.orgId,f.tenant.workspaceId,f.tenant.userId,epoch,expected]);
  const next=await f.select("9007199254740993",false),ack=await next.apply();yes(ack.control.version==="9007199254740993"&&ack.control.outcome==="applied","bigint_exact");
 });
 await check("signed private HTTP adapter preserves exact ACK and rejects body control, kind mismatch and incomplete ACK",async()=>{
  const f=await fixture(),one=await f.select("1",false),key="synthetic-ordered-control-signing";
  const app=createIngestApp({signingKey:key,deliver:{ledger,target:{store:{} as any,keyRoot:"96bulls"}},log:()=>{}});
  const body={account_id:await deliveryAccount(f.tenant),origin_id:f.origin,sessions:[{source:"claude",session_id:"session"}],enabled:false};
  const request=(payload:unknown,c:OrderedControl,path="/v1/delivery/sessions/selection")=>{
   const bytes=new TextEncoder().encode(JSON.stringify(payload)),identity=encodeIdentity({...f.tenant,scopes:[SYNC_SCOPE],control:c}),t=Math.floor(Date.now()/1000);
   return new Request("https://private-fixture"+path,{method:"POST",headers:{[IDENTITY_HEADER]:identity,[SIGNATURE_HEADER]:signRequest(key,t,"POST",path,identity,bytes)},body:bytes});
  };
  const r=await app.fetch(request(body,one.control)),j:any=await r.json();yes(r.status===200&&j.updated===true&&j.control.digest===one.control.digest&&j.control.version==="1","http_exact_control_ack");
  yes((await app.fetch(request({...body,control:one.control},one.control))).status===400,"body_cannot_choose_control");
  yes((await app.fetch(request(body,{...one.control,kind:"reset"}))).status===503,"kind_cannot_cross_route");
  const bad=createIngestApp({signingKey:key,deliver:{ledger:{...ledger,applyOrderedControl:async()=>({updated:1,control:{...one.control,digest:"f".repeat(64),outcome:"applied"}})},target:{store:{} as any,keyRoot:"96bulls"}},log:()=>{}});
  yes((await bad.fetch(request(body,one.control))).status===503,"mismatched_ack_no_success");
 });
 await check("pending PASS or closed admission refuses ordered controls and cannot advance version",async()=>{
  const f=await fixture(),one=await f.select("1",false),before=await f.state();
  await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pending'");
  await refuses(one.apply());await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pass'");
  yes(await f.state()===before,"pending_no_control");
  await owner.query("UPDATE sessions.ingest_buffer_admission SET valid_until=clock_timestamp()-interval '1 second' WHERE org_id=$1",[f.tenant.orgId]);await refuses(one.apply());yes(await f.state()===before,"closed_no_control");
 });
 await check("buffered unversioned controls and direct helper/table grants remain refused",async()=>{
  const f=await fixture(),before=await f.state();await refuses(ledger.setSessionUploads(f.selection(false)));await refuses(ledger.resetUploadedSessions(f.tenant,"old-reset"));
  await refuses(run("SELECT sessions.ingest_apply_ordered_control('{}'::jsonb,'selection')",[]));await refuses(run("SELECT * FROM sessions.ingest_control_receipts",[]));yes(await f.state()===before,"no_unversioned_mutation");
 });
 if(process.env.ORDERED_CONTROL_SOURCE_FIXTURE)await check("actual immutable Neon0077 intent -> signed target -> source completion fences failed target and late duplicate",async()=>{
  const path=process.env.ORDERED_CONTROL_SOURCE_FIXTURE!;
  yes(/^\/home\/khaliqgant\/relay-briefs\/ordered-control-neon-source-[A-Za-z0-9_-]+\/fixture\.json$/.test(path),"owned_source_artifact");
  const artifact=JSON.parse(readFileSync(path,"utf8"));
  yes(artifact.sourceHead==="9f57617874a056b7a82d2324cb3c024b8d9e04be"&&artifact.source0077Sha256==="7fdc2a01b1fd435dccf63b1b18ee17cc0235842d1cbe0353d9cff165e9af6baa"&&
   createHash("sha256").update(artifact.source0077).digest("hex")===artifact.source0077Sha256,"pinned_actual_source");
  const d=JSON.parse(execFileSync("docker",["inspect","ordered-neon-origin-pg17-20261104"],{encoding:"utf8"}))[0];
  yes(d.Config.Labels["com.96bulls.proof"]==="ordered-neon-origin","owned_source_container");
  const port=d.NetworkSettings.Ports["5432/tcp"][0];yes(port.HostIp==="127.0.0.1","source_loopback");
  const sourceCfg=(database:string)=>({host:"127.0.0.1",port:Number(port.HostPort),database,user:"postgres",connectionTimeoutMillis:1000});
  const a=new pg.Client(sourceCfg("postgres"));await a.connect();await a.query("DROP DATABASE IF EXISTS ordered_neon_control_fixture WITH(FORCE)");await a.query("CREATE DATABASE ordered_neon_control_fixture");await a.end();
  sourceOwner=new pg.Client(sourceCfg("ordered_neon_control_fixture"));sourceOwner.on("error",()=>{});await sourceOwner.connect();
  await sourceOwner.query("DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='ordered_neon_runtime') THEN CREATE ROLE ordered_neon_runtime NOLOGIN NOINHERIT NOSUPERUSER;END IF;END$$");
  await sourceOwner.query("BEGIN;CREATE TABLE public.auth_sessions(id text PRIMARY KEY);"+artifact.baselineStatements.join(";")+";UPDATE sessions.upload_cleanup_rollout SET phase=5;COMMIT");
  await sourceOwner.query("BEGIN;"+artifact.source0077+";"+artifact.finalGrantStatement+";COMMIT");
  await sourceOwner.query("SELECT sessions.owner_delivery_cutover_v1('seeding',$1)",[epoch]);
  await sourceOwner.query("SELECT sessions.owner_delivery_cutover_v1('active',$1,repeat('b',64))",[epoch]);
  const source=async(sql:string,args:unknown[])=>{await sourceOwner!.query("BEGIN;SET LOCAL ROLE ordered_neon_runtime");try{const r=await sourceOwner!.query(sql,args);await sourceOwner!.query("COMMIT");return r}catch(e){await sourceOwner!.query("ROLLBACK");throw e}};
  const f=await fixture();
  const begin=async(enabled:boolean)=>(await source("SELECT sessions.set_session_uploads_cutover_v1($1,$2,$3,$4,$5,$6::jsonb,$7) AS r",[epoch,f.tenant.orgId,f.tenant.workspaceId,f.tenant.userId,f.origin,JSON.stringify([{source:"claude",session_id:"session"}]),enabled])).rows[0].r;
  const complete=async(c:any,outcome:string)=>(await source("SELECT sessions.complete_delivery_cutover_control_v1($1,$2,$3,$4,$5::bigint,$6,$7,$8) AS r",[c.epoch,f.tenant.orgId,f.tenant.workspaceId,f.tenant.userId,c.version,c.digest,c.kind,outcome])).rows[0].r;
  const pending=async()=>(await source("SELECT sessions.delivery_cutover_user_pending_v1($1,$2,$3) AS r",[f.tenant.orgId,f.tenant.workspaceId,f.tenant.userId])).rows[0].r;
  const key="synthetic-source-target-key",app=createIngestApp({signingKey:key,deliver:{ledger,target:{store:{} as any,keyRoot:"96bulls"}},log:()=>{}});
  const callTarget=async(enabled:boolean,c:any)=>{
   const control={epoch:c.epoch,version:c.version,digest:c.digest,kind:c.kind} as OrderedControl;
   const body=new TextEncoder().encode(JSON.stringify({account_id:await deliveryAccount(f.tenant),origin_id:f.origin,sessions:[{source:"claude",session_id:"session"}],enabled}));
   const path="/v1/delivery/sessions/selection",identity=encodeIdentity({...f.tenant,scopes:[SYNC_SCOPE],control}),t=Math.floor(Date.now()/1000);
   const r=await app.fetch(new Request("https://actual-target"+path,{method:"POST",headers:{[IDENTITY_HEADER]:identity,[SIGNATURE_HEADER]:signRequest(key,t,"POST",path,identity,body)},body}));
   return{status:r.status,body:await r.json() as any};
  };
  const first=await begin(false);yes(first.version==="1"&&first.pending===true,"source_intent1");
  const retry=await begin(false);yes(retry.version===first.version&&retry.digest===first.digest,"source_reuses_pending");
  await refuses(begin(true)); // Cannot mutate a different source control while unresolved.
  await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pending'");
  const failed=await callTarget(false,first);yes(failed.status===503&&await pending()===true,"failed_target_retains_real_neon_pending");
  yes(await f.deleted()===0,"failed_target_no_fence");
  await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pass'");
  const done=await callTarget(false,first);yes(done.status===200&&done.body.control.digest===first.digest,"actual_source_digest_matches_target");
  yes(await complete(first,done.body.control.outcome)===true&&await pending()===false,"source_ack_complete");
  const second=await begin(true),ack2=await callTarget(true,second);yes(second.version==="2"&&ack2.status===200,"source_second_target");
  const before=await f.state(),late=await callTarget(false,first);yes(late.status===200&&late.body.control.outcome==="stale"&&await f.state()===before,"actual_late_duplicate_no_target_mutation");
  yes(await complete(first,late.body.control.outcome)===true&&await pending()===true,"old_completion_cannot_clear_new_pending");
  yes(await complete(second,ack2.body.control.outcome)===true&&await pending()===false,"current_completion_clears");
  if(process.env.ORDERED_CONTROL_ROUTER_BUNDLE){
   const bundle=process.env.ORDERED_CONTROL_ROUTER_BUNDLE!;
   yes(/^\/home\/khaliqgant\/relay-briefs\/ordered-router-fixture-[A-Za-z0-9_-]+\/router\.mjs$/.test(bundle),"owned_router_bundle");
   const manifest=JSON.parse(readFileSync(bundle.replace("router.mjs","manifest.json"),"utf8"));
   yes(createHash("sha256").update(readFileSync(bundle)).digest("hex")===manifest.bundleSha256,"router_bundle_sha");
   yes(manifest.head==="9f57617874a056b7a82d2324cb3c024b8d9e04be"&&manifest.dirty.length===0,"immutable_actual_router");
   const {createOwnedRouter}=await import(bundle);
   const routerCases:any[]=[];
   const routerCheck=async(name:string,f:()=>Promise<void>)=>{const start=Date.now();await f();routerCases.push({name,pass:true,ms:Date.now()-start})};
   for(const [old,next]of [[false,true],[true,false]])await routerCheck("actual router delayed "+old+" duplicate cannot overwrite "+next,async()=>{
    const f=await fixture();let hold=true,targetCalls=0,seen!:()=>void,release!:()=>void;
    const reached=new Promise<void>(r=>seen=r),gate=new Promise<void>(r=>release=r);
    const privateApp=createIngestApp({signingKey:key,deliver:{ledger,target:{store:{} as any,keyRoot:"96bulls"}},log:()=>{}});
    const env={DATABASE_URL:"owned-source",DELIVERY_CUTOVER:JSON.stringify({version:1,mode:"ingest",epoch,privacyBaselineSha256:"b".repeat(64)}),
     DELIVERY_FORWARD:JSON.stringify({mode:"on",percent:100,onUnreachable:"503"}),DELIVERY_INGEST_SIGNING_KEY:key,
     DELIVERY_INGEST:{async fetch(input:any,init?:any){targetCalls++;const request=input instanceof Request?input:new Request(input,init);if(hold){hold=false;seen();await gate}return privateApp.fetch(request)}}};
    const auth={...f.tenant,tokenSubject:"owned-user",scopes:[SYNC_SCOPE],claims:{}};
    const router=createOwnedRouter((s:string,p:unknown[])=>source(s,p),auth);
    const body=(enabled:boolean)=>({account_id:deliveryAccount(f.tenant),origin_id:f.origin,sessions:[{source:"claude",session_id:"session"}],enabled});
    const call=async(enabled:boolean)=>{const b=body(enabled);return router.fetch(new Request("https://rhc-fixture/v1/delivery/sessions/selection",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({...b,account_id:await b.account_id})}),env)};
    const delayed=call(old);await Promise.race([reached,new Promise((_,reject)=>setTimeout(()=>reject(Error("router_binding_not_reached")),2000))]);
    const retry=await call(old);yes(retry.status===200,"pending_exact_router_retry");
    const newer=await call(next);yes(newer.status===200,"newer_router_control");
    const before=await f.state();release();const late=await delayed;
    yes(late.status===200&&targetCalls===3,"late_router_ack");
    yes(await f.state()===before&&await f.deleted()===(next?0:1),"actual_router_old_duplicate_no_target_mutation");
   });
   await routerCheck("actual router failed target and pre/post batch gates retain pending until exact retry",async()=>{
    const f=await fixture();let targetCalls=0,injectPending=false;let injected:any;
    const privateApp=createIngestApp({signingKey:key,deliver:{ledger,target:{store:{} as any,keyRoot:"96bulls"}},log:()=>{}});
    const env={DATABASE_URL:"owned-source",DELIVERY_CUTOVER:JSON.stringify({version:1,mode:"ingest",epoch,privacyBaselineSha256:"b".repeat(64)}),
     DELIVERY_FORWARD:JSON.stringify({mode:"on",percent:100,onUnreachable:"503"}),DELIVERY_INGEST_SIGNING_KEY:key,
     DELIVERY_INGEST:{async fetch(input:any,init?:any){
      targetCalls++;const request=input instanceof Request?input:new Request(input,init);
      if(injectPending&&new URL(request.url).pathname.endsWith("/batches")){
       injectPending=false;
       injected=(await source("SELECT sessions.set_session_uploads_cutover_v1($1,$2,$3,$4,$5,$6::jsonb,false) AS r",[epoch,f.tenant.orgId,f.tenant.workspaceId,f.tenant.userId,f.origin,JSON.stringify([{source:"claude",session_id:"session"}])])).rows[0].r;
      }
      return privateApp.fetch(request);
     }}};
    const router=createOwnedRouter((s:string,p:unknown[])=>source(s,p),{...f.tenant,tokenSubject:"owned-user",scopes:[SYNC_SCOPE],claims:{}});
    const account=await deliveryAccount(f.tenant),selection={account_id:account,origin_id:f.origin,sessions:[{source:"claude",session_id:"session"}],enabled:false};
    const request=(path:string,body:unknown)=>router.fetch(new Request("https://rhc-fixture/v1/delivery/"+path,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body)}),env);
    await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pending'");
    const refused=await request("sessions/selection",selection);yes(refused.status===503,"actual_failed_target_503");
    const before=targetCalls;
    const batch={protocolVersion:1,batch:{schema_version:1,origin_id:f.origin,batch_id:"router-pending-batch",job_id:"job",generation:1,destination_id:"fixture",instance_id:"fixture",account_id:account,mapping_version:DELIVERY_MAPPING_VERSION,records:[{schema_version:1,origin_id:f.origin,revision_id:"record:1",record_id:"record",revision:1,kind:"session_event",source:"claude",session_id:"session",operation:"upsert",payload:{text:"synthetic pending upload",ts_ms:1761955200000}}]}};
    const blocked=await request("batches",batch);yes(blocked.status===503&&targetCalls===before,"real_new_batch_blocked_before_target");
    const different=await request("sessions/selection",{...selection,enabled:true});yes(different.status===503&&targetCalls===before,"different_pending_no_second_source_or_target");
    await owner.query("UPDATE sessions.__96bulls_role_validation SET status='pass'");
    const retry=await request("sessions/selection",selection);yes(retry.status===200,"actual_router_retry_ack");
    const after=await request("batches",batch);yes(after.status===200&&targetCalls===before+2,"batch_after_exact_control_ack");
    injectPending=true;const raced={...batch,batch:{...batch.batch,batch_id:"router-post-ack-race"}};
    const post=await request("batches",raced);
    yes(post.status===503&&injected?.pending===true,"actual_post_target_pending_gate");
    yes((await owner.query("SELECT state FROM sessions.ingest_batches WHERE org_id=$1 AND batch_id='router-post-ack-race'",[f.tenant.orgId])).rows[0]?.state==="committed","real_target_200_durable_receipt_not_public_ack");
    const fix=await request("sessions/selection",selection);yes(fix.status===200,"post_race_exact_control_retry");
    yes((await request("batches",raced)).status===200,"post_race_replayed_receipt_after_control_ack");
   });
   proof.actualRouter={head:manifest.head,dirty:manifest.dirty,sourceFiles:Object.keys(manifest.businessSourceSha256).length,cases:routerCases,dbShim:manifest.dbShim};
  }
  proof.sourceOracle={head:artifact.sourceHead,source0077Sha256:artifact.source0077Sha256,generatorSha256:artifact.generatorSha256,splitterSha256:artifact.splitterSha256,actualJournalToPrivateTarget:true,actualRhcBatchRouteGate:proof.actualRouter?.dirty.length===0?{head:proof.actualRouter.head,actualBusinessRouter:true,cases:proof.actualRouter.cases.length}:"not exercised"};
 });
 proof.passed=proof.cases.length;proof.maxRSSBytes=process.resourceUsage().maxRSS*1024;
}catch(e){proof.error={stage,code:(e as any)?.code??(e as any)?.message};process.exitCode=1}
finally{await Promise.allSettled([pool.end(),owner.end(),...(sourceOwner?[sourceOwner.end()]:[])]);writeFileSync(out+"/proof.json",JSON.stringify(proof,null,2)+"\n");console.log(JSON.stringify(proof))}
