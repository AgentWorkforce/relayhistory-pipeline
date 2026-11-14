/**
 * Synthetic ONLY: actual Node24 -> disposable owned PostgreSQL17 -> verified
 * filesystem S3 layout -> proof index/RPC -> real ingest. No prod credentials.
 */
import pg from "pg";
import {execFileSync} from "node:child_process";
import {mkdir,readFile,writeFile} from "node:fs/promises";
import {join} from "node:path";
import {performance} from "node:perf_hooks";
import {zstdDecompressSync} from "node:zlib";
import {createHash} from "node:crypto";
import {createFsStore} from "../packages/object-store/src/fs.ts";
import {objectKey,type ObjectStore} from "../src/shared/vendor.ts";
import {objectStoreManifestReader} from "../packages/pipeline/src/lib/pipeline/adapter.ts";
import {normalizeRelayhistoryRecords} from "../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import {readManifestRecords} from "../src/session-read-api/canonical-projection.ts";
import {readerMaxRuleset} from "../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import {runMigrations,generation} from "../src/hoopdb/migrate.ts";
import {seedOrgFromS3} from "../src/session-ingest/seed-s3.ts";
import {SeedProofIndex} from "../src/session-ingest/seed-proof.ts";
import {seedFences,type SeedSql} from "../src/session-ingest/seed.ts";
import {sqlLedger} from "../src/session-ingest/ledger-sql.ts";
import {deliverBatch} from "../src/session-ingest/deliver.ts";
import {prepareDelivery,deliveryAccount} from "../src/session-ingest/vendor/delivery/prepare.ts";
import {DELIVERY_MAPPING_VERSION,type HistoryExportBatch} from "../src/session-ingest/vendor/delivery/delivery-contracts.ts";
import {writeBatchObjects,type WriteResult} from "../src/session-ingest/writer.ts";
import { initScrubEngine } from "../packages/pipeline/src/lib/scrub-engine/index.ts";
// Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
await initScrubEngine();
const root=process.cwd(),out=process.env.SEED_PROOF_OUT??"";
const fail=(code:string):never=>{throw Object.assign(new Error(code),{code})};
const assert=(value:unknown,code:string)=>{if(!value)fail(code)};
if(process.env.SEED_PROOF_DISPOSABLE!=="1"||!out.startsWith("/home/khaliqgant/relay-briefs/seed-proof-real-"))fail("disposable_only");
const container="ingest-seed-proof-pg17-20261104";
const details=JSON.parse(execFileSync("docker",["inspect",container],{encoding:"utf8"}))[0];
assert(details.Config.Labels["com.96bulls.proof"]==="ingest-seed","container_owner");
const mapping=details.NetworkSettings.Ports["5432/tcp"][0];
assert(mapping.HostIp==="127.0.0.1","localhost_only");
const port=Number(mapping.HostPort);
const head=execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim();
const dirty=!!execFileSync("git",["status","--porcelain"],{encoding:"utf8"}).trim();
if(dirty&&!process.argv.includes("--allow-wip"))fail("source_dirty");
const countArg=process.argv.find(v=>v.startsWith("--capacity="))?.split("=")[1];
const capacity=countArg?Number(countArg):0;
assert(Number.isSafeInteger(capacity)&&capacity>=0&&capacity<=1000000,"capacity_bound");
await mkdir(out,{recursive:false,mode:0o700});
const proof:any={head,dirty,node:process.version,arch:process.arch,capacity,startedAt:new Date().toISOString(),cases:[],queries:{metadata:0,source:0},capacityResult:null};
let stage="setup";
const config=(name:string,role="postgres")=>({host:"127.0.0.1",port,user:role,password:"",database:name,ssl:false as const,
 application_name:"seed-proof-"+role,connectionTimeoutMillis:10000,options:"-c statement_timeout=10000 -c lock_timeout=5000"});
const admin=new pg.Client(config("postgres"));await admin.connect();
await admin.query("DROP DATABASE IF EXISTS ingest_seed_fixture WITH (FORCE)");
await admin.query("CREATE DATABASE ingest_seed_fixture");await admin.end();
const owner=new pg.Client(config("ingest_seed_fixture"));
const seeder=new pg.Client({...config("ingest_seed_fixture"),application_name:"seed-proof-seeder"});
const ingest=new pg.Client({...config("ingest_seed_fixture"),application_name:"seed-proof-ingest"});
const blocker=new pg.Client({...config("ingest_seed_fixture"),application_name:"seed-proof-blocker"});
const monitor=new pg.Client({...config("ingest_seed_fixture"),application_name:"seed-proof-monitor"});
let neon:pg.Client|null=null;
const clients=[owner,seeder,ingest,blocker,monitor];
for(const c of clients){c.on("error",()=>{});await c.connect()}
async function check(name:string,work:()=>Promise<void>){stage=name;const start=performance.now();await work();proof.cases.push({name,pass:true,ms:performance.now()-start})}
async function rejected(work:Promise<unknown>,code:string){const e=await work.then(()=>null,e=>e);assert(e?.code===code,"expected_"+code)}
const actor=(client:pg.Client,role:string):SeedSql=>({async query(text,params){
 proof.queries.metadata++;await client.query("BEGIN");
 try{await client.query("SET LOCAL ROLE "+role);const r=await client.query(text,params);await client.query("COMMIT");return r as never}
 catch(e){await client.query("ROLLBACK");throw e}
}});
const seed=actor(seeder,"bulls96_session_ingest_seeder"),live=actor(ingest,"bulls96_session_ingest"),ledger=sqlLedger(live.query as never);
const rpc=async(name:string,p:unknown)=>(await seed.query<{r:any}>(`SELECT sessions.${name}($1::jsonb) AS r`,[JSON.stringify(p)])).rows[0]!.r;
const sleep=(ms:number)=>new Promise(r=>setTimeout(r,ms));
try{
 await runMigrations(owner,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}});
 proof.generation=await generation();
 await owner.query("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='relayhistory_readonly') THEN CREATE ROLE relayhistory_readonly LOGIN NOINHERIT NOSUPERUSER NOCREATEROLE NOCREATEDB NOBYPASSRLS NOREPLICATION; END IF; END $$");
 await owner.query(`CREATE TABLE sessions.deleted_upload_sessions(org_id text,workspace_id text,user_id text,origin_id text,source text,session_id text);
 CREATE TABLE sessions.session_resets(org_id text,workspace_id text,user_id text,request_id text,deleted bigint);
 GRANT USAGE ON SCHEMA sessions TO relayhistory_readonly;GRANT SELECT ON sessions.deleted_upload_sessions,sessions.session_resets TO relayhistory_readonly`);
 neon=new pg.Client(config("ingest_seed_fixture","relayhistory_readonly"));await neon.connect();
 const source={async readOnly<T>(work:(sql:SeedSql)=>Promise<T>){await neon!.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try{const r=await work({query:async(text,params)=>{proof.queries.source++;return await neon!.query(text,params) as never}});await neon!.query("COMMIT");return r}
  catch(e){await neon!.query("ROLLBACK");throw e}
 }};
 const makeBatch=async(tenant:{orgId:string;workspaceId:string},records:any[],id:string):Promise<HistoryExportBatch>=>({
  schema_version:1,origin_id:"origin-1",batch_id:id,job_id:"job",generation:1,destination_id:"dest",instance_id:"inst",
  account_id:await deliveryAccount(tenant),mapping_version:DELIVERY_MAPPING_VERSION,records,
 });
 const rec=(session:string,id:string,revision=7,text="original durable",operation:"upsert"|"delete"="upsert")=>({
  schema_version:1,origin_id:"origin-1",revision_id:"original-"+id+"-"+revision,record_id:id,revision,kind:"session_event" as const,
  source:"claude",session_id:session,operation,payload:operation==="delete"?null:{text,ts_ms:1761955200000},
 });
 let serial=0;
 async function fixture(label:string,records=1){
  const n=++serial,tenant={orgId:"org_"+label,workspaceId:"ws_"+label,userId:"user_"+label},session="session_"+label;
  const dir=join(out,label);await mkdir(dir,{mode:0o700});
  const store=createFsStore({root:join(dir,"objects")}) as unknown as ObjectStore;
  let written:WriteResult|null=null,rawBytes=0;const start=performance.now();
  for(let first=0;first<records;first+=1000){
   const rows=Array.from({length:Math.min(1000,records-first)},(_,i)=>rec(session,"r"+String(first+i).padStart(9,"0"),7,
    records>1?"event "+String(first+i)+" "+"s".repeat(600):"original durable"));
   const prepared=await prepareDelivery(tenant,await makeBatch(tenant,rows,label+"-"+first));
   written=await writeBatchObjects({store,keyRoot:"96bulls",sleep:async()=>{}},{
    ...tenant,originId:prepared.originId,batchDigest:prepared.batchDigest,records:prepared.records});
  }
  const m=written!.manifests[0]!;
  rawBytes=m.manifest.raw.raw_bytes;
  const insert=()=>owner.query(`INSERT INTO sessions.session_index(org_id,workspace_id,harness,session_id,machine_id,user_id,created_at,last_activity_at,
   storage_provider,bucket,manifest_key,content_sha256,raw_bytes,chunk_count,raw_format,revision,trace_id)
   VALUES($1,$2,'claude',$3,$4,$5,now(),now(),'s3','fixture',$6,decode($7,'hex'),$8,$9,'ndjson',$10,$11)`,
   [tenant.orgId,tenant.workspaceId,session,m.manifest.session.machine_id,tenant.userId,m.key,m.manifest.raw.content_sha256,rawBytes,m.manifest.raw.chunks.length,m.manifest.revision,m.manifest.trace_id]);
  await insert();
  await seedFences(source,seed,tenant.orgId,[{workspace_id:tenant.workspaceId,user_id:tenant.userId}]);
  const deps={readerMaxRuleset:readerMaxRuleset(process.env),store,bucket:"fixture",provider:"s3",reader:objectStoreManifestReader(store,{provider:"s3",bucket:"fixture"}),
   prefixFor:(org:string)=>"96bulls/"+org,decompress:(raw:Uint8Array,max:number)=>new Uint8Array(zstdDecompressSync(raw,{maxOutputLength:max}))};
  const args={org_id:tenant.orgId,workspace_id:tenant.workspaceId,source:"claude",session_id:session,generation:proof.generation};
  const capture=async()=>{
   const cap=await rpc("ingest_seed_scope_capture",{...args,mode:"lease"});
   const index=new SeedProofIndex(join(dir,"capture-"+cap.capture_id+".sqlite"),tenant.orgId,head);
   try{for(const pointer of cap.pointers)await index.addPointer(pointer,deps);index.seal(cap.pointers);
    return {cap,payload:{capture_id:cap.capture_id,fingerprint:cap.fingerprint,source_head:head,records:[...index.batches()].flatMap(b=>b.records)}}
   }finally{index.close()}
  };
  return {tenant,session,dir,store,m,insert,deps,args,capture,rawBytes,chunks:m.manifest.raw.chunks.length,producerMs:performance.now()-start};
 }
 await check("readonly source refuses metadata writes",async()=>{
  await rejected(source.readOnly(s=>s.query("INSERT INTO sessions.session_resets VALUES('x','x','x','x',1)")),"25006");
 });
 await check("actual proof seed preserves old winner and receipt IDs",async()=>{
  const f=await fixture("replay");
  const r=await seedOrgFromS3(seed,f.deps,f.tenant.orgId,proof.generation,{sourceHead:head,cacheDir:join(f.dir,"index"),execute:true});
  assert(r.upserted===1,"seed_count");
  const incoming=await makeBatch(f.tenant,[rec(f.session,"r000000000",7,"regenerated changed bytes")],"regenerated");
  incoming.records[0]!.revision_id="actual-regenerated-client-id";
  const result=await deliverBatch({ledger,target:{store:f.store,keyRoot:"96bulls",sleep:async()=>{}}},f.tenant,incoming);
  assert(result.outcome==="replay_fast","expected_replay_fast");
  assert(result.receipt.acceptedRevisionIds[0]==="actual-regenerated-client-id","receipt_id_preserved");
  const rows=normalizeRelayhistoryRecords(await readManifestRecords(f.m.manifest,f.store,"96bulls/"+f.tenant.orgId,f.deps.decompress,Number.POSITIVE_INFINITY,f.deps.readerMaxRuleset));
  assert(rows[0]!.payload?.text==="original durable","stored_winner_preserved");
  const stored=(await owner.query("SELECT revision_id,digest,seed_proof_sha256 FROM sessions.ingest_record_revisions WHERE org_id=$1",[f.tenant.orgId])).rows[0];
  assert(stored.revision_id===null&&stored.digest===null&&stored.seed_proof_sha256,"no_invented_original");
 });
 await check("DELETE/recreate cannot reuse captured source generation",async()=>{
  const f=await fixture("aba"),p=await f.capture();
  await owner.query("DELETE FROM sessions.session_index WHERE org_id=$1",[f.tenant.orgId]);await f.insert();
  await rejected(rpc("ingest_seed_proven_records",p.payload),"40001");
 });
 await check("disable/reenable and empty-ownership reset refuse old proof",async()=>{
  const f=await fixture("disable"),p=await f.capture();
  await ledger.setSessionUploads({tenant:f.tenant,originId:"origin-1",sessions:[["claude",f.session]],enabled:false});
  await ledger.setSessionUploads({tenant:f.tenant,originId:"origin-1",sessions:[["claude",f.session]],enabled:true});
  await rejected(rpc("ingest_seed_proven_records",p.payload),"40001");
  const g=await fixture("reset"),q=await g.capture();
  await ledger.resetUploadedSessions(g.tenant,"reset-without-owned-session");
  await rejected(rpc("ingest_seed_proven_records",q.payload),"40001");
 });
 await check("final wall-clock expiry rolls back writes after a real PK blocker",async()=>{
  const f=await fixture("expiry"),p=await f.capture();
  await blocker.query("BEGIN");
  await blocker.query(`INSERT INTO sessions.ingest_record_revisions(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id)
    VALUES($1,$2,'origin-1','r000000000',1,'blocker-original',repeat('a',64),$3,'claude',$4)`,[f.tenant.orgId,f.tenant.workspaceId,f.tenant.userId,f.session]);
  await owner.query("UPDATE sessions.ingest_seed_captures SET expires_at=clock_timestamp()+interval '300 milliseconds' WHERE id=$1",[p.cap.capture_id]);
  const start=performance.now(),attempt=rpc("ingest_seed_proven_records",p.payload).then(()=>null,e=>e);
  let waiting=false;
  for(let i=0;i<100;i++){waiting=(await monitor.query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='seed-proof-seeder' AND wait_event_type='Lock') AS n")).rows[0].n;if(waiting)break;await sleep(10)}
  assert(waiting,"actual_blocker_witness");
  await sleep(400);await blocker.query("COMMIT");
  const error=await attempt;assert(error?.code==="55000","final_expiry_refused");
  const row=(await owner.query("SELECT revision::text,revision_id FROM sessions.ingest_record_revisions WHERE org_id=$1",[f.tenant.orgId])).rows[0];
  assert(row.revision==="1"&&row.revision_id==="blocker-original","rollback_preserves_unrelated_row");
  await sleep(100);
  assert((await monitor.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name='seed-proof-seeder' AND state<>'idle'")).rows[0].n===0,"no_late_active_query");
  proof.expiry={ms:performance.now()-start,blockerWitness:true,rollback:true,noLateQuery:true};
 });
 await check("PG17 PUBLIC MAINTAIN/column rights and inherited operator edge are refused",async()=>{
  const verifier=await readFile(join(root,"supabase/bootstrap/session_ingest_seeder_verify.sql"),"utf8");
  await owner.query("GRANT MAINTAIN ON sessions.ingest_seed_captures TO PUBLIC");await rejected(owner.query(verifier),"55000");
  await owner.query("REVOKE MAINTAIN ON sessions.ingest_seed_captures FROM PUBLIC");
  await owner.query("GRANT SELECT(id) ON sessions.ingest_seed_captures TO PUBLIC");await rejected(owner.query(verifier),"55000");
  await owner.query("REVOKE SELECT(id) ON sessions.ingest_seed_captures FROM PUBLIC");
  await owner.query("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='relayhistory_owner') THEN CREATE ROLE relayhistory_owner LOGIN NOINHERIT; END IF; END $$");
  await owner.query("GRANT bulls96_session_ingest_seeder TO relayhistory_owner WITH INHERIT TRUE, SET TRUE");await rejected(owner.query(verifier),"55000");
  await owner.query("GRANT bulls96_session_ingest_seeder TO relayhistory_owner WITH INHERIT FALSE, SET TRUE");await owner.query(verifier);
 });
 if(capacity)await check("bounded real raw producer to proof index to SQL capacity",async()=>{
  const f=await fixture("capacity",capacity),before={...proof.queries},start=performance.now();
  const r=await seedOrgFromS3(seed,f.deps,f.tenant.orgId,proof.generation,{sourceHead:head,cacheDir:join(f.dir,"index"),execute:true});
  const elapsed=performance.now()-start;
  const actual=(await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_record_revisions WHERE org_id=$1",[f.tenant.orgId])).rows[0].n;
  assert(r.records===capacity&&actual===capacity&&r.upserted===capacity,"capacity_exact_count");
  proof.capacityResult={records:capacity,rawBytes:f.rawBytes,chunks:f.chunks,producerMs:f.producerMs,seedMs:elapsed,
    recordsPerSecond:capacity/(elapsed/1000),metadataQueries:proof.queries.metadata-before.metadata,result:r};
 });
 proof.finishedAt=new Date().toISOString();proof.maxRSSBytes=process.resourceUsage().maxRSS*1024;proof.passed=proof.cases.length;proof.failed=0;
}catch(error){
 proof.failed=1;proof.error={stage,code:(error as any)?.code??"proof_failed"};process.exitCode=1;
}finally{
 for(const c of [...clients,...(neon?[neon]:[])])await c.end().catch(()=>{});
 await writeFile(join(out,"proof.json"),JSON.stringify(proof,null,2)+"\n");
 console.log(JSON.stringify({head,dirty,passed:proof.passed??proof.cases.length,failed:proof.failed,capacity:proof.capacityResult,error:proof.error}));
}
