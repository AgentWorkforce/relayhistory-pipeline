/** Owned real source559 -> RHC router -> signed receiverdb7 -> PG17/MinIO. */
import pg from "pg";
import {execFileSync} from "node:child_process";import {readFileSync,writeFileSync,mkdirSync} from "node:fs";import {createHash} from "node:crypto";import assert from "node:assert/strict";
import {runMigrations,generation} from "../src/hoopdb/migrate.ts";
import {SemanticPostgresExecutor} from "../src/search-features/semantic-copy-postgres.ts";
import {sqlLedger} from "../src/session-ingest/ledger-sql.ts";
import {createIngestApp} from "../src/session-ingest/app.ts";
import {createS3Store} from "../src/shared/vendor.ts";
import {deliveryAccount,prepareDelivery as targetPrepare} from "../src/session-ingest/vendor/delivery/prepare.ts";
import {DELIVERY_MAPPING_VERSION} from "../src/session-ingest/vendor/delivery/delivery-contracts.ts";
import {signRequest} from "../packages/object-store/src/sigv4.ts";
import { initScrubEngine } from "../packages/pipeline/src/lib/scrub-engine/index.ts";
// Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
await initScrubEngine();
const target=process.cwd(),source="/home/khaliqgant/Projects/AgentWorkforce/relayhistory-cloud-worktrees/delivery-current-custody-witness-20261104",sourceHead="559ca6268146ccbc22195c90899b2cf4ed44332f";
const artifact=process.env.LAZY_CUSTODY_SOURCE_BUNDLE??"";
if(process.env.LAZY_CUSTODY_DISPOSABLE!=="1"||!artifact.startsWith("/home/khaliqgant/relay-briefs/ordered-router-fixture-lazy"))throw Error("owned_only");
const actualHead=execFileSync("git",["-C",target,"rev-parse","HEAD"],{encoding:"utf8"}).trim();
const sourceDirty=execFileSync("git",["-C",target,"status","--porcelain","--","src","supabase","packages","stacks"],{encoding:"utf8"}).trim();
if(sourceDirty)throw Error("tracked_business_source_dirty");
const {createOwnedRouter,CURRENT_CUSTODY_AUTHORITY_SQL,prepareDelivery:sourcePrepare}=await import(artifact+"/router.mjs");
const manifest=JSON.parse(readFileSync(artifact+"/manifest.json","utf8"));assert.equal(manifest.head,sourceHead);assert.deepEqual(manifest.dirty,[]);
const sha=(x:string|Uint8Array)=>createHash("sha256").update(x).digest("hex");
assert.equal(sha(readFileSync(artifact+"/router.mjs")),manifest.bundleSha256);
function port(name:string,label:string,key:string){const d=JSON.parse(execFileSync("docker",["inspect",name],{encoding:"utf8"}))[0];assert.equal(d.Config.Labels["com.96bulls.proof"],label);const p=d.NetworkSettings.Ports[key][0];assert.equal(p.HostIp,"127.0.0.1");return Number(p.HostPort)}
const targetPort=port("lazy-current-pair-pg17-20261104","lazy-current-pair","5432/tcp"),sourcePort=port("ordered-neon-origin-pg17-20261104","ordered-neon-origin","5432/tcp"),s3Port=port("buffer-gc-runtime-minio-20261104","buffer-gc-runtime","9000/tcp");
const cfg=(port:number,database:string,user="postgres")=>({host:"127.0.0.1",port,database,user,connectionTimeoutMillis:1000,application_name:"lazy-current-pair"});
const suffix=Date.now(),targetDb="lazy_target_"+suffix,sourceDb="lazy_source_"+suffix,role="lazy_source_"+suffix;
for(const [port,name,builtin] of [[targetPort,targetDb,false],[sourcePort,sourceDb,true]] as const){const a=new pg.Client(cfg(port,"postgres"));await a.connect();if(!builtin){for(const r of(await a.query("SELECT datname FROM pg_database WHERE datname LIKE 'lazy_target_%'")).rows)await a.query("DROP DATABASE "+r.datname+" WITH(FORCE)")}await a.query("CREATE DATABASE "+name+(builtin?" LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8' TEMPLATE template0":""));if(builtin)await a.query("CREATE ROLE "+role+" LOGIN NOINHERIT NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS");await a.end()}
const owner=new pg.Client(cfg(targetPort,targetDb)),sourceOwner=new pg.Client(cfg(sourcePort,sourceDb));await owner.connect();await sourceOwner.connect();
let sourceClient:pg.Client|undefined,pool:pg.Pool|undefined;
const out="/home/khaliqgant/relay-briefs/lazy-current-pair-"+suffix;mkdirSync(out,{mode:0o700});const result:any={targetHead:actualHead,sourceHead,node:process.version,sourceManifest:manifest,targetDb,sourceDb,cases:[]};let stage="setup";
async function check(name:string,f:()=>Promise<void>){stage=name;const at=Date.now();await f();result.cases.push({name,pass:true,ms:Date.now()-at});console.log("PASS "+name)}
try{
 await runMigrations(owner,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}});
 const expected=await generation();result.generation=expected;
 await owner.query(readFileSync(target+"/supabase/bootstrap/session_ingest_role.sql","utf8").split("\n").filter(x=>!x.startsWith("\\")).join("\n"));
 // Read exact source migrations using the source's own applier, never copied SQL.
 const {readMigrations,migrationStatements}=await import(source+"/packages/relayhistory/scripts/apply-neon-migration.mjs");
 const ms=readMigrations();for(const m of ms){const b=execFileSync("git",["-C",source,"show",sourceHead+":packages/relayhistory/db/migrations/"+m.name]);assert.equal(sha(b),m.checksum)}
 const baseline=ms.filter((m:any)=>!m.name.startsWith("0077_"));
 await sourceOwner.query("BEGIN;CREATE TABLE public.auth_sessions(id text PRIMARY KEY);"+migrationStatements(baseline,role).join(";")+";UPDATE sessions.upload_cleanup_rollout SET phase=5;COMMIT");
 const m77=ms.find((m:any)=>m.name.startsWith("0077_"));await sourceOwner.query("BEGIN;"+m77.statements.join(";")+";"+migrationStatements(ms,role).find((s:string)=>s.startsWith("DO $runtime_grants$"))+";COMMIT");
 for(const r of(await sourceOwner.query("SELECT index_name,definition FROM sessions.delivery_rollout_indexes()")).rows)await sourceOwner.query("CREATE INDEX "+r.index_name+" "+r.definition);
 await sourceOwner.query("SELECT sessions.activate_delivery_catalog();SELECT sessions.activate_delivery_projection_v2()");
 const tenant={orgId:"lazy-real-org",workspaceId:"ws-real",userId:"user-real"},origin="origin-real",epoch="3".repeat(64),privacy="a".repeat(64);
 const rec=(id:string,revision:number,session:string|null,text:string):any=>({schema_version:1,origin_id:origin,revision_id:id+":"+revision+":"+sha(text).slice(0,8),record_id:id,revision,kind:session?"session_event":"presence",source:"claude",session_id:session,operation:"upsert",payload:{text,ts_ms:1761955200000,large_numeric:9007199254740992,unicode:"Σİ😀é"}});
 const batch=async(id:string,records:any[]):Promise<any>=>({schema_version:1,origin_id:origin,batch_id:id,job_id:"job-"+id,generation:1,destination_id:"fixture",instance_id:"fixture",account_id:await deliveryAccount(tenant),mapping_version:DELIVERY_MAPPING_VERSION,records});
 const legacy=rec("old-native",8,"B","actual stored old8B");const peerLegacy=rec("peer-native",8,"P","actual peer old8");const legacyBatch=await batch("legacy8",[legacy,peerLegacy]);const prepared=await sourcePrepare(tenant,legacyBatch);
 await sourceOwner.query("SELECT sessions.accept_delivery_batch($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb)",[tenant.orgId,tenant.workspaceId,tenant.userId,origin,legacyBatch.batch_id,prepared.batchDigest,JSON.stringify(prepared.records),JSON.stringify(prepared.receipt)]);
 const stored=(await sourceOwner.query("SELECT digest,revision::text,session_id FROM sessions.delivery_records WHERE record_id='old-native'")).rows[0];
 assert.equal(stored.digest,prepared.records[0].digest);assert.equal((await targetPrepare(tenant,legacyBatch)).records[0].digest,stored.digest);

 const peers=(revision:number)=>[{...rec("peer-native",revision,"P","Z peer"),revision_id:"z-peer-"+revision},{...rec("peer-native",revision,"P","A peer"),revision_id:"a-peer-"+revision}];
 async function oldAccept(id:string,records:any[]){const b=await batch(id,records),p=await sourcePrepare(tenant,b);return sourceOwner.query("SELECT sessions.accept_delivery_batch($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb)",[tenant.orgId,tenant.workspaceId,tenant.userId,origin,id,p.batchDigest,JSON.stringify(p.records),JSON.stringify(p.receipt)])}
 await check("actual original source SQL below-current peers ACK ignored; equal/above peers409",async()=>{
  await oldAccept("oracle-below",peers(1));assert.equal((await sourceOwner.query("SELECT revision::text FROM sessions.delivery_records WHERE record_id='peer-native'")).rows[0].revision,"8");
  for(const rev of [8,9])await assert.rejects(()=>oldAccept("oracle-conflict-"+rev,peers(rev)),(e:any)=>e.code==="P0001"&&e.message==="delivery_revision_conflict");
 });
 await check("actual original source sorts competing catalog native records a then z despite input z,a",async()=>{
  const rows=[{...rec("z-oracle",1,"catalog-oracle","Z"),kind:"session",payload:{title:"Z wins"}},{...rec("a-oracle",1,"catalog-oracle","A"),kind:"session",payload:{title:"A first SQL"}}];
  await oldAccept("oracle-order",rows);
  assert.equal((await sourceOwner.query("SELECT title FROM sessions.session_catalog WHERE org_id=$1 AND workspace_id=$2 AND session_id='catalog-oracle'",[tenant.orgId,tenant.workspaceId])).rows[0].title,"Z wins");
 });
 const sourceReceiptsBeforeCutoff=(await sourceOwner.query("SELECT count(*)::int AS n FROM sessions.delivery_receipts")).rows[0].n;
 await sourceOwner.query("SELECT sessions.owner_delivery_cutover_v1('seeding',$1)",[epoch]);
 const seq=(await sourceOwner.query("SELECT c.oid::text AS oid,n.nspname||'.'||c.relname AS name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=ANY(ARRAY['sessions.delivery_catalog_acceptance_seq'::regclass,'sessions.delivery_projection_acceptance_seq'::regclass]) ORDER BY c.relname")).rows;
 const sequences=[];for(const r of seq){const x=(await sourceOwner.query("SELECT last_value::text,is_called FROM "+r.name)).rows[0];sequences.push({sequenceRegclass:r.name,sequenceOid:r.oid,allocatedHighWater:x.last_value,isCalled:x.is_called,incrementBy:"1",cycle:false})}
 const floor=sequences.reduce((n:any,s:any)=>BigInt(n)>BigInt(s.allocatedHighWater)?n:s.allocatedHighWater,"0");
 await sourceOwner.query("SELECT sessions.owner_delivery_cutover_v1('active',$1,$2)",[epoch,privacy]);
 sourceClient=new pg.Client(cfg(sourcePort,sourceDb,role));await sourceClient.connect();
 const identity=(await sourceClient.query("WITH a AS("+CURRENT_CUSTODY_AUTHORITY_SQL+") SELECT encode(sha256(convert_to(value::text,'UTF8')),'hex') AS sha FROM a",["fixture.original.invalid"])).rows[0].sha;
 const baselineProof=sha(JSON.stringify({provider:"owned-postgres",fixtureId:sourceDb,sourceIdentitySha256:identity,sequences,floor,epoch}));
 await owner.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)",[JSON.stringify({baselineMode:"lazy-current",generation:expected,sourceIdentitySha256:identity,controlEpoch:epoch,privacyBaselineSha256:privacy,retentionContractSha256:"b".repeat(64),sourceProofSha256:baselineProof,validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"})]);
 await owner.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify({protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:sequences,allocatedHighWater:floor,generation:expected,sourceIdentitySha256:identity,sourceProofSha256:baselineProof})]);
 pool=new pg.Pool({...cfg(targetPort,targetDb,"bulls96_session_ingest"),max:4});const executor=new SemanticPostgresExecutor(pool);
 const run=(s:string,p:unknown[])=>executor.queryBounded<Record<string,unknown>>(s,p,{signal:AbortSignal.timeout(5000),deadline:Date.now()+5000});
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:expected});
 const endpoint="http://127.0.0.1:"+s3Port,bucket="lazy-current-pair-"+suffix,credentials={accessKeyId:"gc-fixture-access",secretAccessKey:"gc-fixture-only-key-20261104"};
 async function s3(method:string,query="",body=""){const url=new URL(endpoint+"/"+bucket+query),headers=new Headers({"content-type":"application/xml"});await signRequest({method,url,headers,payloadSha256:sha(body),region:"us-east-1",credentials});const r=await fetch(url,{method,headers,...(body?{body}:{}),signal:AbortSignal.timeout(5000)});assert.equal(r.ok,true,"fixture_s3_"+r.status)}
 await s3("PUT");await s3("PUT","?versioning",'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>');
 const store=createS3Store({bucket,region:"us-east-1",endpoint,credentials}),key="synthetic-lazy-current-signing";
 const app=createIngestApp({signingKey:key,deliver:{ledger,acceptanceMode:"buffered",target:{store,keyRoot:"96bulls",sleep:async()=>{}}},log:(fields:any)=>{console.log("TARGET",JSON.stringify(fields))}});
 let targetCalls=0,lastPrivate:any,lastResponse:Response|undefined;
 const router=createOwnedRouter((s:string,p:unknown[])=>sourceClient!.query(s,p),{...tenant,scopes:["rth:sync"]});
 const env={PIPELINE_RAW_S3_BUCKET:bucket,PIPELINE_RAW_S3_ENDPOINT:endpoint,PIPELINE_RAW_S3_REGION:"us-east-1",PIPELINE_RAW_S3_ACCESS_KEY_ID:credentials.accessKeyId,PIPELINE_RAW_S3_SECRET_ACCESS_KEY:credentials.secretAccessKey,DATABASE_URL:"postgres://fixture.original.invalid/owned-source",DELIVERY_CUTOVER:JSON.stringify({version:1,mode:"ingest",epoch,privacyBaselineSha256:privacy}),DELIVERY_FORWARD:JSON.stringify({mode:"on",percent:100,onUnreachable:"503"}),DELIVERY_CUSTODY_SOURCE:JSON.stringify({version:1,hostname:"fixture.original.invalid",sourceIdentitySha256:identity}),DELIVERY_INGEST_SIGNING_KEY:key,DELIVERY_INGEST:{fetch:async(input:Request|string,init?:RequestInit)=>{const req=input instanceof Request?input:new Request(input,init);targetCalls++;lastPrivate=JSON.parse(await req.clone().text());lastResponse=await app.fetch(req);return lastResponse}}};
 const call=async(path:string,body:any)=>{const r=await router.request("/v1"+path,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body)},env);const j=await r.json();if(r.status!==200)console.log("REFUSAL",path,r.status,JSON.stringify(j),lastResponse?.status);assert.equal(r.status,200);return j as any};
 const send=async(id:string,records:any[])=>call("/delivery/batches",{protocolVersion:1,batch:await batch(id,records)});
 const seats=async(id:string)=>(await owner.query("SELECT sessions.ingest_receipt_seats_for_stage(active_stage_token) AS r FROM sessions.ingest_buffer_outbox WHERE batch_id=$1",[id])).rows[0]?.r;
 await check("actual source stored digest equals target candidate; exact equal revision replay is ignored with exact SQL seal",async()=>{
  await send("same8",[legacy]);const r=(await seats("same8")).records[0];assert.equal(r.candidateDigest,stored.digest);assert.equal(r.firstDigest,stored.digest);assert.equal(r.bodyAccepted,false);assert.equal(lastPrivate.witness.records[0].current.digest,stored.digest);assert.ok(lastResponse!.headers.has("x-relay-custody-ack"));
 });
 await check("changed equal revision and lower different session remain permanently ignored",async()=>{
  await send("changed8",[{...legacy,payload:{...legacy.payload,text:"changed equal8"}}]);const changed=(await seats("changed8")).records[0];assert.notEqual(changed.candidateDigest,stored.digest);assert.equal(changed.firstDigest,stored.digest);assert.equal(changed.bodyAccepted,false);
  await send("lowerA",[rec("old-native",1,"A","ignored lower")]);assert.equal((await seats("lowerA")).records[0].bodyAccepted,false);
 });
 await check("actual source qualified eraseB/PRE/controlseal then fresh1C succeeds without old Neon reseed",async()=>{
  await call("/delivery/sessions/selection",{account_id:await deliveryAccount(tenant),origin_id:origin,sessions:[{source:"claude",session_id:"B"}],enabled:false});
  assert.equal(lastPrivate.witness.privacyVersion,"0");assert.equal(lastPrivate.witness.pending,true);
  await send("freshC",[rec("old-native",1,"C","fresh after erase")]);assert.equal((await seats("freshC")).records[0].bodyAccepted,true);assert.equal(lastPrivate.witness.sourceControlVersion,"1");assert.equal(lastPrivate.witness.privacy,null);
  assert.equal((await seats("lowerA")).records[0].bodyAccepted,false);
 });
 await check("actual source-native within-batch rank overrides public input ordinal",async()=>{
  const z={...rec("z-target",1,"rank","Z"),kind:"session",payload:{title:"Z wins"}},a={...rec("a-target",1,"rank","A"),kind:"session",payload:{title:"A first SQL"}};
  await send("rank-ZA",[z,a]);const r=(await seats("rank-ZA")).records;assert.equal(lastPrivate.witness.records[0].processingRank,1);assert.equal(lastPrivate.witness.records[1].processingRank,0);assert.equal(r[0].bodyAccepted,true);assert.equal(r[1].bodyAccepted,true);assert.ok(BigInt(r[0].seat)>BigInt(r[1].seat));
 });
 await check("actual RHC/target repeated native key matches source current-revision oracle",async()=>{
  await send("target-below",peers(1));assert.ok((await seats("target-below")).records.every((r:any)=>r.bodyAccepted===false));
  for(const rev of [8,9]){const r=await router.request("/v1/delivery/batches",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({protocolVersion:1,batch:await batch("target-conflict-"+rev,peers(rev))})},env);assert.equal(r.status,409);assert.equal((await r.json() as any).error.code,"delivery_conflict")}
 });

 await check("actual no-op enable derives SQL seal and preserves existing current custody",async()=>{
  await call("/delivery/sessions/selection",{account_id:await deliveryAccount(tenant),origin_id:origin,sessions:[{source:"claude",session_id:"C"}],enabled:true});
  assert.equal(lastPrivate.witness.sourceControlVersion,"2");assert.ok(lastResponse!.headers.has("x-relay-custody-ack"));
  assert.equal((await owner.query("SELECT current_session FROM sessions.ingest_lazy_native_slots WHERE record_id='old-native'")).rows[0].current_session,"C");
 });
 await check("actual all-fenced suppressed and committed replay still derive fresh SQL privacy seal",async()=>{
  await send("suppressed-B",[rec("fenced-old",1,"B","must not persist")]);assert.ok(lastResponse!.headers.has("x-relay-custody-ack"));assert.equal(lastPrivate.witness.records[0].candidateFenced,true);
  assert.equal((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE batch_id='suppressed-B'")).rows[0].n,0);
  await send("suppressed-B",[rec("fenced-old",1,"B","must not persist")]);assert.ok(lastResponse!.headers.has("x-relay-custody-ack"));
 });
 async function other(auth:any,path:string,body:any){
  const rr=createOwnedRouter((s:string,p:unknown[])=>sourceClient!.query(s,p),{...auth,scopes:["rth:sync"]});
  const r=await rr.request("/v1"+path,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body)},env);
  const j=await r.json();if(r.status!==200)console.log("OTHER REFUSAL",r.status,j);assert.equal(r.status,200);return j as any;
 }
 await check("actual cold zero-owned-row reset retains permanent USER seal after source completes",async()=>{
  const auth={...tenant,userId:"cold-zero-reset"};
  const r=await other(auth,"/delivery/sessions/reset",{account_id:await deliveryAccount(auth),request_id:"empty-reset",confirmation:"DELETE ALL SESSIONS"});
  assert.equal(r.deleted,0);assert.equal(lastPrivate.witness.privacyVersion,"0");assert.equal(lastPrivate.witness.pending,true);assert.ok(lastResponse!.headers.has("x-relay-custody-ack"));
  const seal=(await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_lazy_user_seals WHERE user_id=$1",[auth.userId])).rows[0].n;assert.equal(seal,1);
 });
 await check("actual cold retired-only batch seals USER and qualified new org uses explicit source evidence",async()=>{
  const auth={orgId:"new-qualified-org",workspaceId:"new-workspace",userId:"new-user"};
  const b={...await batch("new-org-retired",[{...rec("retired",1,"S","discard retired evidence"),kind:"trajectory"}]),account_id:await deliveryAccount(auth)};
  await other(auth,"/delivery/batches",{protocolVersion:1,batch:b});
  assert.equal(lastPrivate.witness.originalRecordCount,1);assert.equal(lastPrivate.witness.candidateCount,0);
  assert.deepEqual(lastPrivate.witness.privacy,{complete:true,resets:[],deletions:[]});
  assert.ok(lastResponse!.headers.has("x-relay-custody-ack"));
  assert.equal((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_lazy_user_seals WHERE org_id=$1",[auth.orgId])).rows[0].n,1);
  assert.equal((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_lazy_native_slots WHERE org_id=$1",[auth.orgId])).rows[0].n,0);
 });
 await check("source only reads metadata after cutover; no canonical revision seed or legacy new receipt",async()=>{
  assert.equal((await sourceOwner.query("SELECT count(*)::int AS n FROM sessions.delivery_receipts")).rows[0].n,sourceReceiptsBeforeCutoff);assert.equal((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions")).rows[0].n,0);assert.ok(targetCalls>=6);
 });
 result.status="GREEN";result.targetCalls=targetCalls;result.sourceIdentitySha256=identity;result.storage={bucket,versioned:true};result.limit="Owned builtinPG17/Node24 real RHC router source559 and receiver exact targetHead, no production or workerd claim";
}catch(e:any){result.status="RED";result.stage=stage;result.error={name:e?.name,code:e?.code,message:e?.message};process.exitCode=1;console.error(e)}
finally{writeFileSync(out+"/proof.json",JSON.stringify(result,null,2));console.log(out);await pool?.end();await sourceClient?.end();await sourceOwner.end();await owner.end()}
