/** Disposable owned Node24 / PG17 / versioned MinIO publication proof. */
import pg from 'pg';
import {runCanonicalPublicationRuntime}from'../src/session-ingest/publication-runtime.ts';
import{execFileSync}from'node:child_process';
import{readFileSync,mkdirSync,writeFileSync}from'node:fs';
import{runMigrations,generation}from'../src/hoopdb/migrate.ts';
import{createS3Store,objectKey,readAll,objectStoreManifestReader,prefixOrgResolver,rawChunkKey,manifestKey,parseSessionKey,type ObjectStore}from'../src/shared/vendor.ts';
import{signRequest}from'../packages/object-store/src/sigv4.ts';
import{sha256Hex}from'../packages/object-store/src/crypto.ts';
import{xmlElements,xmlText}from'../packages/object-store/src/xml.ts';
import{createS3Eraser}from'../src/migration/neon-s3-erasure/eraser.ts';
import{SemanticPostgresExecutor}from'../src/search-features/semantic-copy-postgres.ts';
import{sqlLedger}from'../src/session-ingest/ledger-sql.ts';
import{deliverBatch}from'../src/session-ingest/deliver.ts';
import{publicationRpc}from'../src/session-ingest/publication-rpc.ts';
import{publishBufferedStage}from'../src/session-ingest/publication-job.ts';
import{publicationGcLedger,collectPublicationObject}from'../src/session-ingest/publication-gc.ts';
import{writeBatchObjects}from'../src/session-ingest/writer.ts';
import{createFsStore}from'../packages/object-store/src/fs.ts';
import{indexPointer,droppedFanout}from'../src/session-pipeline/pointer.ts';
import{readCanonicalScope}from'../src/session-read-api/canonical-projection.ts';
import{readerMaxRuleset}from'../packages/pipeline/src/lib/scrub-engine/reader-policy.ts';
import{zstdDecompressSync}from'node:zlib';
import{deliveryAccount,prepareDelivery}from'../src/session-ingest/vendor/delivery/prepare.ts';
import{DELIVERY_MAPPING_VERSION,type HistoryExportBatch}from'../src/session-ingest/vendor/delivery/delivery-contracts.ts';
const out=process.env.PUBLICATION_PROOF_OUT??'';
if(process.env.PUBLICATION_DISPOSABLE!=='1'||!out.startsWith('/home/khaliqgant/relay-briefs/canonical-publication-'))throw Error('owned_disposable_only');
function port(name:string,key:string){const d=JSON.parse(execFileSync('docker',['inspect',name],{encoding:'utf8'}))[0];
 if(d.Config.Labels['com.96bulls.proof']!=='canonical-publisher')throw Error('container_owner');const p=d.NetworkSettings.Ports[key][0];if(p.HostIp!=='127.0.0.1')throw Error('loopback_only');return Number(p.HostPort);}
mkdirSync(out,{recursive:false,mode:0o700});
const proof:any={head:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),dirty:!!execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim(),node:process.version,cases:[],failed:0};
const pgPort=port('canonical-publisher-pg17-20261104','5432/tcp'),endpoint='http://127.0.0.1:'+port('canonical-publisher-minio-20261104','9000/tcp');
const bucket='canonical-publisher-fixture',region='us-east-1',credentials={accessKeyId:'publisher-fixture-access',secretAccessKey:'publisher-fixture-only-key-20261104'};
const assert=(v:unknown,c:string)=>{if(!v)throw Error(c)};
const cfg=(database:string,user='postgres')=>({host:'127.0.0.1',port:pgPort,database,user,connectionTimeoutMillis:1000,application_name:'canonical-publication-proof'});
const admin=new pg.Client(cfg('postgres'));await admin.connect();
for(const row of(await admin.query("SELECT datname FROM pg_database WHERE datname LIKE 'publication\_%' ESCAPE '\\'")).rows){if(!/^publication_[a-z0-9_]+$/.test(row.datname))throw Error('owned_database_name');await admin.query('DROP DATABASE '+row.datname+' WITH(FORCE)');}
const database='publication_owned_fixture';await admin.query('CREATE DATABASE '+database);await admin.end();
const owner=new pg.Client(cfg(database));owner.on('error',()=>{});await owner.connect();
let pool:pg.Pool|undefined,stage='setup';
const store=createS3Store({endpoint,bucket,region,credentials});
const eraser=createS3Eraser({endpoint,bucket,region,credentials});
async function s3(method:string,suffix='',body=new Uint8Array()){
 const url=new URL(endpoint+'/'+bucket+suffix),headers=new Headers({'content-type':'application/xml'});
 await signRequest({method,url,headers,payloadSha256:await sha256Hex(body),region,credentials});
 const r=await fetch(url,{method,headers,...(body.byteLength?{body}:{}),signal:AbortSignal.timeout(5000)});
 if(!r.ok)throw Error('fixture_s3_'+r.status);return r;
}
async function versions(key:string){const xml=await(await s3('GET','?versions=&prefix='+encodeURIComponent(key))).text();assert(xmlText(xml,'IsTruncated')==='false','inventory_truncated');
 const rows=[];for(const tag of['Version','DeleteMarker'])for(const e of xmlElements(xml,tag))if(xmlText(e,'Key')===key)rows.push({id:xmlText(e,'VersionId'),latest:xmlText(e,'IsLatest')==='true',marker:tag==='DeleteMarker'});return rows;}
async function check(name:string,work:()=>Promise<void>){stage=name;const at=Date.now();await work();proof.cases.push({name,ms:Date.now()-at,pass:true});}
function proxied(s:ObjectStore,put:ObjectStore['put']):ObjectStore{return new Proxy(s,{get(t,k){if(k==='put')return put;const v=Reflect.get(t,k);return typeof v==='function'?v.bind(t):v;}})}
try{
 await runMigrations(owner,{profile:{mode:'local',shim:true,bootstrap:true,verify:true,runtimeRole:'postgres'}});
 const expected=await generation();proof.schemaGeneration=expected;
 const bootstrap=readFileSync('supabase/bootstrap/session_ingest_role.sql','utf8').split('\n').filter(l=>!l.startsWith('\\')).join('\n');await owner.query(bootstrap);
 await owner.query(readFileSync('supabase/bootstrap/session_ingest_verify.sql','utf8'));
 await s3('HEAD').catch(async e=>{if(e.message!=='fixture_s3_404')throw e;await s3('PUT')});
 await s3('PUT','?versioning',new TextEncoder().encode('<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>'));
 assert((await(await s3('GET','?versioning')).text()).includes('<Status>Enabled</Status>'),'versioning');
 pool=new pg.Pool({...cfg(database,'bulls96_session_ingest'),max:3});pool.on('error',()=>{});
 const bounded=new SemanticPostgresExecutor(pool),run=(sql:string,args:unknown[])=>bounded.queryBounded<any>(sql,args,{signal:AbortSignal.timeout(5000),deadline:Date.now()+5000});
 const producer=sqlLedger(run,{acceptanceMode:'buffered',expectedGeneration:expected});
 const target={bucket,region,keyRoot:'96bulls',generation:expected,provider:'s3' as const},rpc=publicationRpc(bounded,target),gc=publicationGcLedger(bounded,target);
 const runId=crypto.randomUUID().slice(0,8);let serial=0;
 async function fixture(){const id=++serial,tenant={orgId:'pub-'+runId+'-'+id,workspaceId:'ws-'+id,userId:'user-'+id},origin='origin-'+id;
  await owner.query('BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder');try{await owner.query('SELECT sessions.ingest_seed_fence_baseline($1::jsonb)',[JSON.stringify({org_id:tenant.orgId,workspace_id:tenant.workspaceId,user_id:tenant.userId,deletions:[],resets:[]})]);await owner.query('COMMIT');}catch(e){await owner.query('ROLLBACK');throw e}
  await owner.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,valid_until,schema_generation)VALUES($1,repeat('a',64),repeat('b',64),clock_timestamp()+interval'1 hour',$2)",[tenant.orgId,expected]);
  await owner.query("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,$4,repeat('c',64),repeat('d',64),clock_timestamp()+interval'1 hour',$5,$6,'96bulls',$7)",[tenant.orgId,tenant.workspaceId,tenant.userId,origin,bucket,region,expected]);
  async function deliver(revision=1,operation:'upsert'|'delete'='upsert'){const batch:HistoryExportBatch={schema_version:1,origin_id:origin,batch_id:'batch-'+id+'-'+revision,job_id:'job-'+id,generation:1,destination_id:'fixture',instance_id:'fixture',account_id:await deliveryAccount(tenant),mapping_version:DELIVERY_MAPPING_VERSION,
   records:[{schema_version:1,origin_id:origin,revision_id:'revision-'+revision,record_id:'record-'+id,revision,kind:'session_event',source:'claude',session_id:'session-'+id,operation,payload:operation==='delete'?null:{text:'private synthetic revision '+revision,ts_ms:1761955200000}}]};
   const result=await deliverBatch({ledger:producer,acceptanceMode:'buffered',target:{store,keyRoot:'96bulls'}},tenant,batch);assert(result.outcome==='accepted_buffered','buffered_ack');
   return(await owner.query('SELECT active_stage_token token FROM sessions.ingest_buffer_outbox WHERE org_id=$1 AND batch_id=$2',[tenant.orgId,batch.batch_id])).rows[0].token as string;
  }
  const disable=()=>producer.setSessionUploads({tenant,originId:origin,sessions:[['claude','session-'+id]],enabled:false});
  const enable=()=>producer.setSessionUploads({tenant,originId:origin,sessions:[['claude','session-'+id]],enabled:true});
  const row=async()=>(await owner.query('SELECT*FROM sessions.session_index WHERE org_id=$1',[tenant.orgId])).rows[0];
  return{tenant,origin,deliver,disable,enable,row};
 }
 await check('real Node24 bounded nonowner PG transport and versioned S3 publish atomically install pointer/revision/job',async()=>{
  const f=await fixture(),stage=await f.deliver();const r=await publishBufferedStage({rpc,target,store:()=>store},stage,{enabled:true,maxJobMs:30000,heartbeatMs:100});assert(r.enabled&&r.images===1&&r.records===1,'publication_result');
  const p=await f.row();assert(p.manifest_key.includes('/publications/'),'immutable_pointer');assert((await versions(p.manifest_key)).length===1,'single_manifest_version');
  const manifest=JSON.parse(new TextDecoder().decode(await readAll(await store.get(objectKey(p.manifest_key)))));assert(manifest.revision===1&&manifest.raw.epoch===0,'revision_not_scrub_epoch');
  assert((await owner.query('SELECT count(*)::int n FROM sessions.pipeline_jobs WHERE org_id=$1',[f.tenant.orgId])).rows[0].n===1,'atomic_job');
 });
 for(const waitAt of ['raw','manifest'])await check('disable erase enable before delayed '+waitAt+' PUT leaves persistent barrier and fresh generation survives',async()=>{
  const f=await fixture(),stage=await f.deliver();let release!:()=>void,reached!:()=>void;const gate=new Promise<void>(r=>release=r),hit=new Promise<void>(r=>reached=r);let lateKey='';
  const paused=proxied(store,async(key,body,opts)=>{if(!lateKey&&(waitAt==='raw'||String(key).endsWith('/manifest.json'))){lateKey=String(key);reached();await gate;}return store.put(key,body,opts)});
  const pending=publishBufferedStage({rpc,target,store:()=>paused},stage,{enabled:true,maxJobMs:30000,heartbeatMs:100}).then(v=>({v}),e=>({e}));
  await hit;assert((await owner.query('SELECT count(*)::int n FROM sessions.ingest_publication_objects WHERE object_key=$1',[lateKey])).rows[0].n===1,'intent_before_late_put');
  await f.disable();await f.enable();const tickets=(await gc.claim(100,AbortSignal.timeout(10000))).filter(t=>t.key.startsWith('96bulls/'+f.tenant.orgId+'/'));
  assert(tickets.some(t=>t.key===lateKey),'missing_object_gc_intent');
  for(const t of tickets)assert(await collectPublicationObject(t,{ledger:gc,target,store,eraser},AbortSignal.timeout(15000)),'gc_finish');
  release();const result=await pending;assert('e'in result,'late_job_refused');
  assert(!(await f.row()),'no_missing_manifest_ABA');
  const currentBytes=await readAll(await store.get(objectKey(lateKey)));assert(await sha256Hex(currentBytes)!==tickets.find(t=>t.key===lateKey)!.sha256,'late_private_bytes_reappeared');
  const remaining=await versions(lateKey);assert(remaining.length===1&&remaining[0]!.latest&&!remaining[0]!.marker,'permanent_barrier_only');
  const marker=JSON.parse(new TextDecoder().decode(currentBytes));assert(marker.kind==='upload-generation-redaction','no_old_private_body');
  const nextStage=await f.deliver(2);const fresh=await publishBufferedStage({rpc,target,store:()=>store},nextStage,{enabled:true,maxJobMs:30000});assert(fresh.enabled&&fresh.images===1,'fresh_publication');
  const current=await f.row();assert(current.manifest_key!==lateKey&&current.deleted_at===null,'fresh_generation_current');
  const freshBody=await readAll(await store.get(objectKey(current.manifest_key)));assert(freshBody.byteLength>100,'fresh_bytes_retained');
  assert((await gc.claim(100,AbortSignal.timeout(10000))).every(t=>t.uploadGeneration!==current.manifest_key.split('/g=')[1]?.split('/')[0]),'no_live_generation_gc');
 });


 for(const variant of ['same-carrier-live-base','separate-export-carrier'])await check('authoritative mixed canonical fold '+variant+' keeps old records and native revision/tombstone semantics',async()=>{
  const f=await fixture(),id=f.tenant.orgId.split('-').at(-1)!;
  const oldBatch:HistoryExportBatch={schema_version:1,origin_id:f.origin,batch_id:'historical-'+id,job_id:'legacy-job',generation:1,destination_id:'fixture',instance_id:'fixture',account_id:await deliveryAccount(f.tenant),mapping_version:DELIVERY_MAPPING_VERSION,
   records:['record-'+id,'legacy-only-'+id].map((record_id,i)=>({schema_version:1,origin_id:f.origin,revision_id:'legacy-rev-'+i,record_id,revision:1,kind:'session_event',source:'claude',session_id:'session-'+id,operation:'upsert',payload:{text:i?'preserved historical record':'old superseded transcript',ts_ms:1761955200000+i,role:'assistant',message_id:'native-legacy-'+i}}))};
  const prepared=await prepareDelivery(f.tenant,oldBatch),legacyStore=createFsStore({root:out+'/historical-'+id});
  const built=await writeBatchObjects({store:legacyStore,keyRoot:'96bulls'},{...f.tenant,originId:f.origin,batchDigest:prepared.batchDigest,records:prepared.records});
  const original=built.manifests[0]!,oldLocation=parseSessionKey(original.key)!;
  const legacy=structuredClone(original.manifest),location={...oldLocation};
  if(variant==='separate-export-carrier'){
   location.machineId='legacy-machine-'+id;legacy.session.machine_id=location.machineId;legacy.writer.delivery_instance_id=location.machineId;
   legacy.raw.owner='migration:synthetic-grant';legacy.raw.epochs[0]!.opened_by=legacy.raw.owner;
   legacy.extensions={...legacy.extensions,'client.relayhistory_export':{version:1,source:'neon.delivery_records',grant_id:'synthetic-grant',plan_digest:'e'.repeat(64),source_user_id:f.tenant.userId}};
  }
  for(const c of legacy.raw.chunks){const body=await readAll(await legacyStore.get(rawChunkKey(oldLocation,c.seq,c.start_offset,c.sha256,'ndjson')));
   await store.put(rawChunkKey(location,c.seq,c.start_offset,c.sha256,'ndjson'),body,{ifNoneMatch:true});}
  const oldKey=String(manifestKey(location));const ack=await store.put(objectKey(oldKey),new TextEncoder().encode(JSON.stringify(legacy)),{ifNoneMatch:true});
  const reader=objectStoreManifestReader(store,{provider:'s3',bucket});
  await indexPointer({key:oldKey,provider:'s3',bucket,etag:ack.version.eTag,version:ack.version.version},{db:{pool,query:async(t:string,p?:unknown[])=>owner.query(t,p)}as any,lock:async(_scope,work)=>work(),reader,exists:async k=>!!await store.head(objectKey(k)),orgs:prefixOrgResolver,allowPrefix:()=>true,fanout:droppedFanout});
  for(const r of prepared.records)await owner.query("INSERT INTO sessions.ingest_record_revisions(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",[f.tenant.orgId,f.tenant.workspaceId,f.origin,r.record_id,r.revision,r.revision_id,r.digest,f.tenant.userId,r.source,r.session_id]);
  const next=await f.deliver(2);await publishBufferedStage({rpc,target,store:()=>store},next,{enabled:true,maxJobMs:30000});
  const scope={orgId:f.tenant.orgId,workspaceId:f.tenant.workspaceId,harness:'claude',sessionId:'session-'+id};
  const fold=async()=>{
   const keys=(await owner.query('SELECT manifest_key FROM sessions.session_index WHERE org_id=$1 AND deleted_at IS NULL',[f.tenant.orgId])).rows.map(p=>p.manifest_key);
   return readCanonicalScope(scope,keys,{readerMaxRuleset:readerMaxRuleset(process.env),reader,store,prefixFor:()=> '96bulls/'+f.tenant.orgId,decompress:(bytes,maxOutputLength)=>new Uint8Array(zstdDecompressSync(bytes,{maxOutputLength}))},{maxScopeBytes:16777216,maxScopeEvents:100,maxRows:100,insertBatch:50},1,scope.sessionId);
  };
  const before=await fold();assert(before.complete&&!before.partial,'mixed_scope_complete');
  const contents=before.projectionRows.filter(r=>r.table==='convergence_events').map(r=>r.row.content);
  assert(contents.includes('preserved historical record')&&contents.includes('private synthetic revision 2')&&!contents.includes('old superseded transcript'),'authoritative_revision_fold');
  const deletion=await f.deliver(3,'delete');await publishBufferedStage({rpc,target,store:()=>store},deletion,{enabled:true,maxJobMs:30000});
  const after=await fold(),afterText=after.projectionRows.filter(r=>r.table==='convergence_events').map(r=>r.row.content);
  assert(after.complete&&afterText.length===1&&afterText[0]==='preserved historical record','canonical_tombstone_across_formats');
 });
 await check('actual permanent runtime validates compiled schema and dedicated role before publish and generation GC',async()=>{
  const f=await fixture();await f.deliver();
  const env={AWS_REGION:'us-east-1',HOOPDB_DB_MODE:'supabase',INGEST_PUBLISH_ENABLED:'true',INGEST_PUBLICATION_GC_ENABLED:'true',
   INGEST_PUBLISH_BUCKET:bucket,INGEST_PUBLISH_EXPECT_GENERATION:expected,INGEST_PUBLISH_AWS_ACCOUNT_ID:'111122223333',INGEST_PUBLISH_SUPABASE_PROJECT_REF:'abcdefghijklmnopqrst',INGEST_PUBLISH_INSTANCE_ID:'i-0123456789abcdef0',INGEST_PUBLISH_SECRET_ARN:'arn:aws:secretsmanager:us-east-1:111122223333:secret:96bulls/test/canonical-publisher-abcdef',
   INGEST_PUBLISH_KMS_KEY_ARN:'arn:aws:kms:us-east-1:111122223333:key/12345678-1234-1234-1234-123456789abc',INGEST_PUBLISH_INSTANCE_PROFILE_ARN:'arn:aws:iam::111122223333:instance-profile/owned-fixture',INGEST_PUBLISH_ROLE_NAME:'owned-fixture',INGEST_PUBLISH_BUDGET_MS:'30000'};
  const overrides={credentials:async()=>credentials,secret:async()=>JSON.stringify({hoopdb_url:'postgresql://bulls96_session_ingest.abcdefghijklmnopqrst:synthetic-unused@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require'}),
   pool:()=>new pg.Pool({...cfg(database,'bulls96_session_ingest'),max:4}),store:()=>store,eraser:()=>eraser};
  const compiled=process.env.PUBLICATION_COMPILED_BUNDLE;
  if(compiled&&!compiled.startsWith('/home/khaliqgant/relay-briefs/canonical-publication-'))throw Error('owned_bundle_path');
  const runtime=compiled?(await import('file://'+compiled)).runCanonicalPublicationRuntime:runCanonicalPublicationRuntime;
  const r=await runtime(env,'publish',overrides);assert(r.enabled&&r.published>=1,'actual_runtime_publish');
  const p=await f.row();assert(p?.deleted_at===null,'runtime_pointer');
  await f.disable();await f.enable();
  const erased=await runtime(env,'gc',overrides);assert(erased.enabled&&erased.finished>0,'actual_runtime_gc');
  const marker=JSON.parse(new TextDecoder().decode(await readAll(await store.get(objectKey(p.manifest_key)))));assert(marker.kind==='upload-generation-redaction','runtime_barrier');
  proof.compiledBundle=compiled??null;
 });
 proof.peakRss=process.resourceUsage().maxRSS*1024;proof.pass=true;
}catch(e){proof.failed++;proof.stage=stage;proof.code=(e as any)?.code??null;proof.error=(e as Error).message;process.exitCode=1;}
finally{await pool?.end();await owner.end();writeFileSync(out+'/result.json',JSON.stringify(proof,null,2)+'\n');console.log(JSON.stringify(proof));}
