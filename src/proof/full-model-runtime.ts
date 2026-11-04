/** Owned synthetic Node24 -> full migrator -> dedicated LOGIN -> FINAL BUNDLE
 * -> local HTTP Responses fixture + versioned MinIO. No production ports/keys.
 * READY metadata/ScopeKeys setup is a620 PORT fixture, not its full creator.
 * Run Node24 --experimental-transform-types --import ./scripts/proof-typescript-loader.mjs.
 * MODEL_PROOF_BUNDLE must identify the build manifest's exact clean source. */
import assert from"node:assert/strict";
import pg from"pg";
import{execFileSync}from"node:child_process";
import{readFileSync,writeFileSync,mkdirSync}from"node:fs";
import{randomUUID,randomBytes,createHash}from"node:crypto";
import{createServer}from"node:http";
import{resolve,dirname}from"node:path";
import{pathToFileURL}from"node:url";
import{runMigrations,generation}from"../hoopdb/migrate.ts";
import{exportReplayFixture,REPLAY_FIXTURE_IDENTITY as identity}from"../session-pipeline/__tests__/replay-fixture.ts";
import{createS3Store,type ObjectStore}from"../../packages/object-store/src/index.ts";
import{signRequest}from"../../packages/object-store/src/sigv4.ts";
import{sha256Hex}from"../../packages/object-store/src/crypto.ts";
import{captureHistorySource,captureHistorySourceReference,readHistorySummary}from"../shared/history-source-authority.ts";
import{sqlHistoryMaskStore,updateHistoryMask}from"../shared/history-mask.ts";
import{historyRecordContentSha256}from"../shared/history-visibility.ts";
import{produceCanonicalReplay}from"../session-pipeline/replay-publication.ts";
import{masterKeyring,parseMasterKeys}from"../shared/keyring.ts";
import{readFullModelSummary}from"../bob-costas/full-model-artifacts.ts";
import type{RelayhistoryStreamRecord}from"../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
const bundle=resolve(process.env.MODEL_PROOF_BUNDLE??""),out=resolve(process.env.MODEL_PROOF_OUT??"");
if(process.env.MODEL_PROOF_DISPOSABLE!=="1"||!bundle.startsWith("/home/khaliqgant/relay-briefs/bob-model-runtime-build-")||!bundle.endsWith("/bundle/index.mjs")||!out.startsWith("/home/khaliqgant/relay-briefs/bob-model-runtime-proof-"))throw Error("owned_fixture_only");
const manifest=JSON.parse(readFileSync(resolve(dirname(bundle),"../manifest.json"),"utf8"));
const head=execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim();
assert.equal(manifest.head,head);assert.equal(manifest.dirty,false);
assert.equal(createHash("sha256").update(readFileSync(bundle)).digest("hex"),manifest.bundleSha256);
assert.equal(execFileSync("git",["status","--porcelain"],{encoding:"utf8"}).trim(),"");
mkdirSync(out,{recursive:false,mode:0o700});
const long=process.env.MODEL_PROOF_SMALL!=="1";
const proof:any={head,manifest,node:process.version,scenario:long?"long":"small",cases:[],providerCalls:0,requests:0,intentsBeforePut:0,kmsHeaders:0,packagedInvocations:0,manual620PortFixture:true};
const check=(name:string)=>{proof.cases.push(name);console.log("PASS "+name)};
const owned="full-model-proof-"+process.pid+"-"+Date.now(),containers:string[]=[],pools:pg.Pool[]=[];
const credentials={accessKeyId:"owned-model-fixture",secretAccessKey:randomBytes(32).toString("hex")},region="us-east-1",bucket="replay-fixture";
const docker=(args:string[])=>execFileSync("docker",args,{encoding:"utf8"}).trim();
function launch(suffix:string,image:string,port:number,args:string[],options:string[]=[]){
 const name=owned+"-"+suffix;
 docker(["run","-d","--name",name,"--label","com.96bulls.proof="+owned,"--memory","1536m","--cpus","2","-p","127.0.0.1::"+port,...options,image,...args]);containers.push(name);
 return Number(docker(["port",name,String(port)]).match(/:(\d+)$/)![1]);
}
const wait=async(fn:()=>Promise<boolean>)=>{const end=Date.now()+45000;while(Date.now()<end){if(await fn().catch(()=>false))return;await new Promise(r=>setTimeout(r,100))}throw Error("owned_fixture_startup")};
let provider:ReturnType<typeof createServer>|undefined,owner:pg.Pool|undefined;
try{
 const port=launch("pg17","pgvector/pgvector:pg17",5432,[],["-e","POSTGRES_HOST_AUTH_METHOD=trust","--tmpfs","/var/lib/postgresql/data:rw,size=768m"]);
 owner=new pg.Pool({host:"127.0.0.1",port,user:"postgres",database:"postgres",connectionTimeoutMillis:1000,max:2});pools.push(owner);
 await wait(async()=>{await owner!.query("SELECT 1");return true});
 // Wait for the entrypoint's temporary init server to exit before migrating.
 await wait(async()=>docker(["logs",containers[0]!]).includes("PostgreSQL init process complete"));
 await wait(async()=>{await owner!.query("SELECT 1");return true});
 const migration=await owner.connect();try{await runMigrations(migration,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}})}finally{migration.release()}
 const login=readFileSync("supabase/bootstrap/session_history_model_role.sql","utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
 const verify=readFileSync("supabase/bootstrap/session_history_model_verify.sql","utf8");
 await owner.query(verify);await owner.query(login);await owner.query(verify);
 await owner.query(readFileSync("supabase/bootstrap/session_ingest_role.sql","utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n"));
 const s3port=launch("minio","cgr.dev/chainguard/minio:latest",9000,["server","/data","--address",":9000"],["--entrypoint","/usr/bin/minio","--user","65532","-e","MINIO_ROOT_USER="+credentials.accessKeyId,"-e","MINIO_ROOT_PASSWORD="+credentials.secretAccessKey,"--tmpfs","/data:rw,uid=65532,gid=65532,size=768m"]);
 const endpoint="http://127.0.0.1:"+s3port;
 await wait(async()=>(await fetch(endpoint+"/minio/health/ready")).ok);
 async function s3(method:string,suffix="",body=""){
  const url=new URL(endpoint+"/"+bucket+suffix),headers=new Headers({"content-type":"application/xml"}),bytes=new TextEncoder().encode(body);
  await signRequest({method,url,headers,payloadSha256:await sha256Hex(bytes),region,credentials});
  const r=await fetch(url,{method,headers,...(body?{body:bytes}:{}),signal:AbortSignal.timeout(5000)});assert.equal(r.ok,true);return r;
 }
 await s3("PUT");await s3("PUT","?versioning",'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>');
 const store=createS3Store({endpoint,bucket,region,credentials});
 // Bind the exact real S3 bucket used by the fixture exporter.
 const x={db:owner,pg:{exec:(q:string)=>owner!.query(q)}};
 const gen=await generation(),authority=randomUUID(),scope={orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId};
 const base="2026-11-01T10:00:00.000000Z",records:RelayhistoryStreamRecord[]=[];
 for(let i=0;i<31;i++)records.push({kind:"convergence_event",origin_id:"machine:machine:0",record_id:"native-"+i,revision:"1",accepted_seq:String(i+1),operation:"upsert",received_at:base,payload:{source:identity.source,session_id:identity.sessionId,machine_id:"machine:0",user_id:"user_replay",event_id:"native-"+i,kind:"session_event",type:"assistant",ts:base,ingested_at:base,actor_name:"fixture",actor_role:"assistant",content:i===23?"long all-content ".repeat(long?160000:1):"message-"+i,model:"fixture",tool_calls:i===23?Array.from({length:long?10001:2},(_,n)=>({id:n,args:{name:"tool",surrogate:"\ud800"}})):[],record:{payload:{message_id:"real-native-"+i,arbitrary:["kept",i],native_exact:9007199254740993n}}}});
 const visible:RelayhistoryStreamRecord={kind:"session_event",origin_id:"current-origin",record_id:"visible-native",revision:"1",accepted_seq:"9007199254740993",operation:"upsert",received_at:base,payload:{text:"VISIBLE-MASKED-EVIDENCE",role:"assistant",ts_ms:Date.parse(base),message_id:"visible-message"}};
 const hidden:RelayhistoryStreamRecord={...visible,record_id:"hidden-native",payload:{...visible.payload,text:"MOVED-PRIVATE-MUST-NOT-MODEL"}};
 records.push(visible,hidden);
 await exportReplayFixture(store,x.db,0,1,records);
 // Exact role boundary for mask registration and later job IO.
 const role=(name:string)=>{const p=new pg.Pool({host:"127.0.0.1",port,user:name,database:"postgres",connectionTimeoutMillis:1000,max:1});pools.push(p);return{query:async<T>(q:string,args?:unknown[])=>({rows:(await p.query(q,args)).rows as T[]})}};
 const ingest=role("bulls96_session_ingest");
 const root=await updateHistoryMask(sqlHistoryMaskStore(ingest,gen,identity.orgId,identity.workspaceId),{orgId:identity.orgId,workspaceId:identity.workspaceId},null,[
  {recordId:"visible-native",winner:{originId:visible.origin_id,recordId:visible.record_id,kind:visible.kind,revision:visible.revision,acceptedSeq:visible.accepted_seq!,operation:"upsert",userId:"user_replay",source:identity.source,sessionId:identity.sessionId,contentSha256:historyRecordContentSha256(visible),lineage:"0"}},
  {recordId:"hidden-native",winner:null}]);
 const anchor="96bulls/"+identity.orgId+"/upload-generations/g="+authority+"/v1/ws="+identity.workspaceId+"/m=history_scope/h=claude/d=2026/11/01/s=logical/publications/"+authority+"/manifest.json";
 await x.db.query("INSERT INTO sessions.ingest_history_scope_versions(authority_id,org_id,workspace_id,harness,session_id,source_epoch,mask_sha256,record_count,derived_anchor_key,bucket,region,key_root,state,pointer_snapshot_sha256) SELECT $1,org_id,workspace_id,harness,session_id,epoch,$2,2,$3,'replay-fixture','us-east-1','96bulls','ready',repeat('a',64) FROM sessions.session_replay_publications WHERE org_id=$4",[authority,root.sha256,anchor,identity.orgId]);
 await x.db.query("INSERT INTO sessions.ingest_history_scope_current VALUES($1,$2,$3,$4,$5,true)",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId,authority]);
 await x.db.query("UPDATE sessions.ingest_history_scope_versions SET source_fingerprint=encode(sha256(convert_to(sessions.session_replay_source_snapshot(org_id,workspace_id,harness,session_id)::text,'UTF8')),'hex') WHERE authority_id=$1",[authority]);
 for(const recordId of["visible-native","hidden-native"])await x.db.query("INSERT INTO sessions.ingest_history_scope_keys VALUES($1,$2,$3,$4,$5)",[identity.orgId,identity.workspaceId,recordId,identity.source,identity.sessionId]);
 const history=await captureHistorySource(ingest,identity,{schemaGeneration:gen});
 // Uses restricted publisher once its managed-capture follow-up is composed.
 const graph=await produceCanonicalReplay(scope,{db:ingest,store,history,provider:"s3",bucket:"replay-fixture",prefix:{PIPELINE_KEY_PREFIX:"96bulls",PIPELINE_ORG_FROM_PREFIX:"true"}});
 assert.equal(graph.published,true);
 assert.equal((await x.db.query("SELECT * FROM sessions.history_model_wakeups")).rows.length,1);

 check("official export/masked graph creates one durable wake using restricted ingest role");
 assert.equal(gen,manifest.generation);
 const keyText="fixture:"+randomBytes(32).toString("hex"),keyring=await masterKeyring(parseMasterKeys(keyText));
 let providerMode="normal";const observed:string[]=[];
 provider=createServer(async(req,res)=>{
  let text="";for await(const c of req)text+=c;
  const body=JSON.parse(text);assert.equal(req.method,"POST");assert.equal(body.store,false);assert.equal(body.text.format.type,"json_schema");assert.equal(body.text.format.strict,true);
  assert(Buffer.byteLength(text)<=128*1024);proof.providerCalls++;observed.push(body.input);
  const value=body.text.format.name==="session_full_summary"?{short:"Owned HTTP complete summary",long:"Owned complete evidence result including message30 and large tool input",state:"done",stateReason:"offline suggestion only"}:{notes:"Owned bounded evidence unit"};
  const output=providerMode==="refusal"?[{content:[{type:"refusal",refusal:"fixture"}]}]:[{content:[{type:"output_text",text:JSON.stringify(value)}]}];
  res.writeHead(200,{"content-type":"application/json"});res.end(JSON.stringify({status:"completed",model:"fixture-model-v1",usage:{input_tokens:100,output_tokens:20},output}));
 });
 await new Promise<void>(r=>provider!.listen(0,"127.0.0.1",r));const providerUrl="http://127.0.0.1:"+(provider.address()as any).port;
 const actualJob=(await import(pathToFileURL(bundle).href)).runFullModelJob;
 const api=async(...args:any[])=>{proof.packagedInvocations++;return actualJob(...args)};
 const env={SESSION_FULL_MODEL_ENABLED:"true",BOB_COSTAS_MODEL:"fixture-model",BOB_COSTAS_MODEL_TIMEOUT_MS:"1000",AWS_REGION:region,HOOPDB_DB_MODE:"supabase",PIPELINE_RAW_S3_BUCKET:bucket,SESSION_MODEL_KMS_KEY_ARN:"arn:aws:kms:us-east-1:941433935742:key/11111111-1111-4111-8111-111111111111",SESSION_MODEL_SECRET_ARN:"arn:aws:secretsmanager:us-east-1:941433935742:secret:96bulls/proof/session-model-abcdef",SESSION_MODEL_EXPECT_GENERATION:gen};
 process.env.AWS_ACCESS_KEY_ID="ASIA"+"X".repeat(16);process.env.AWS_SECRET_ACCESS_KEY="synthetic-only";process.env.AWS_SESSION_TOKEN="synthetic-managed-token";
 let crash=true;const jobPools:pg.Pool[]=[];
 const bridge=(async(input:any,init?:RequestInit)=>{
  const request=new Request(input,init),original=new URL(request.url);assert.equal(original.hostname,bucket+".s3.us-east-1.amazonaws.com");proof.requests++;
  const data=new Uint8Array(await request.arrayBuffer()),headers=new Headers(request.headers),key=decodeURIComponent(original.pathname.slice(1));
  if(request.method==="PUT"){
   assert.equal(headers.get("x-amz-server-side-encryption"),"aws:kms");assert.equal(headers.get("x-amz-server-side-encryption-aws-kms-key-id"),env.SESSION_MODEL_KMS_KEY_ARN);assert.equal(headers.get("if-none-match"),"*");
   const intent=(await owner!.query("SELECT object_key FROM sessions.ingest_history_derived_objects WHERE object_key=$1 AND sha256=$2 AND bytes=$3",[key,createHash("sha256").update(data).digest("hex"),data.length])).rows[0];
   assert(intent);proof.intentsBeforePut++;proof.kmsHeaders++;
  }
  for(const h of["authorization","host","x-amz-security-token","x-amz-server-side-encryption","x-amz-server-side-encryption-aws-kms-key-id"])headers.delete(h);
  const url=new URL(endpoint+"/"+bucket+original.pathname+original.search);await signRequest({method:request.method,url,headers,payloadSha256:await sha256Hex(data),region,credentials});
  const result=await fetch(url,{method:request.method,headers,...(data.length?{body:data}:{}),signal:request.signal});
  if(crash&&request.method==="PUT"){crash=false;await result.body?.cancel();throw Error("owned_lost_put_response")}
  return new Response(result.body,{status:result.status,headers:result.headers});
 })as typeof fetch;
 const providerFetch=(async(input:any,init?:RequestInit)=>{assert.equal(String(input),"https://api.openai.com/v1/responses");return fetch(providerUrl,init)})as typeof fetch;
 const overrides={secret:async()=>JSON.stringify({hoopdb_url:"postgres://bulls96_session_model.pyamkrhoyszgzirqqffq:unused@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require",openai_api_key:"owned-fixture-not-live",summary_master_keys:keyText}),
 pool:(c:pg.PoolConfig)=>{assert.equal(c.host,"aws-0-us-east-1.pooler.supabase.com");assert.equal((c.ssl as any).rejectUnauthorized,true);const p=new pg.Pool({host:"127.0.0.1",port,database:"postgres",user:"bulls96_session_model",connectionTimeoutMillis:1000,max:1});jobPools.push(p);return p},fetcher:bridge,providerFetch};
 const run=async()=>api(env,()=>300000,overrides);
 assert.equal((await api({},()=>300000,{secret:async()=>{throw Error("off_io")}})).enabled,false);check("packaged default OFF performs no credential/SQL/provider IO");
 const first=await run();assert.equal(first.failed,1);
 const failed=(await owner.query("SELECT checkpoint,status FROM sessions.history_model_work")).rows[0];assert.equal(failed.status,"pending");assert.equal(failed.checkpoint.calls,"0");assert.equal(failed.checkpoint.cursor.rowOrdinal,"0");
 assert.equal((await owner.query("SELECT count(*)::int n FROM sessions.ingest_history_derived_objects WHERE object_key LIKE '%/bob-slice/%'")).rows[0].n,1);
 check("lost PUT response retains known intent and original durable cursor; no fake completion");
 await owner.query("UPDATE sessions.history_model_work SET next_attempt_at=clock_timestamp()");
 let complete=0;
 for(let i=0;i<12&&!complete;i++){const r=await run();complete+=r.completed;assert.equal(r.failed,0)}
 assert.equal(complete,1);assert(proof.providerCalls>(long?10:0));assert(observed.some(s=>s.includes("message-30")));assert(observed.some(s=>s.includes("VISIBLE-MASKED-EVIDENCE")));assert(observed.every(s=>!s.includes("MOVED-PRIVATE-MUST-NOT-MODEL")));assert(observed.some(s=>JSON.parse(s).fragments?.some((f:any)=>f.jsonFragment.includes('"native_exact":9007199254740993'))));
 assert.equal((await owner.query("SELECT status FROM sessions.history_model_work")).rows[0].status,"complete");
 for(const kind of["short","long"]as const){const pointer=await readHistorySummary(ingest,history,kind);assert(pointer);const text=await readFullModelSummary({store,keyring,captured:history,pointer,signal:AbortSignal.timeout(5000)});assert(typeof text?.summary==="string"&&text.summary.includes("Owned"));const object=await store.getObject(pointer.objectKey as never);const chunks=[];for await(const c of object.body as any)chunks.push(c);const bytes=Buffer.concat(chunks);assert.equal(createHash("sha256").update(bytes).digest("hex"),pointer.objectSha256);assert.equal(bytes.length,pointer.objectBytes);assert(!bytes.includes(Buffer.from("Owned complete")))}
 assert.equal((await owner.query("SELECT count(*)::int n FROM sessions.ingest_history_states")).rows[0].n,0);
 check("final packaged job uses actual HTTP structured Responses/all long rows and both authenticated encrypted summaries");
 assert(jobPools.every(p=>(p as any).ending===true));check("all owned job and heartbeat pools close after every invocation");

 const {enqueueFullModelWork,claimFullModelWork,modelWorkConfig}=await import("../bob-costas/full-model-ledger.ts");
 const modelPool=new pg.Pool({host:"127.0.0.1",port,user:"bulls96_session_model",database:"postgres",connectionTimeoutMillis:1000,max:1});pools.push(modelPool);
 const modelDb={query:async<T>(q:string,p?:unknown[])=>({rows:(await modelPool.query(q,p)).rows as T[]})};
 const compact=await captureHistorySourceReference(modelDb,identity,{schemaGeneration:gen});
 const key=await keyring.currentKey({orgId:identity.orgId,workspaceId:identity.workspaceId});
 await enqueueFullModelWork(modelDb,compact,modelWorkConfig("fixture-refusal",key.keyId,graph.publication!));
 providerMode="refusal";
 const refused=await api({...env,BOB_COSTAS_MODEL:"fixture-refusal"},()=>300000,overrides);
 assert.equal(refused.failed,1);assert.equal(refused.completed,0);
 const refusal=(await owner.query("SELECT status,last_error_code FROM sessions.history_model_work WHERE config->>'model'='fixture-refusal'")).rows[0];
 assert.equal(refusal.status,"failed");assert.equal(refusal.last_error_code,"model_refusal");
 providerMode="normal";check("actual provider refusal is durable terminal failure, never fake empty completion");
 if(!long){
  await enqueueFullModelWork(modelDb,compact,modelWorkConfig("fixture-upgrade",key.keyId,graph.publication!));
  const oldTicket=(await claimFullModelWork(modelDb,gen,"fixture-upgrade"))!;assert(oldTicket);
  const e={generation:"d".repeat(64),identity:compact.identity,source:compact.source,authorityId:compact.authorityId,maskSha256:compact.maskSha256};
  await owner.query("UPDATE sessions.history_model_work SET evidence=$1::jsonb,captured=jsonb_set(captured,'{schemaGeneration}',to_jsonb($2::text)),work_key=encode(sha256(convert_to(jsonb_build_array('history-model-work-v1',$1::jsonb,config)::text,'UTF8')),'hex')WHERE work_id=$3",[JSON.stringify(e),"d".repeat(64),oldTicket.workId]);
  const waiting=await api({...env,BOB_COSTAS_MODEL:"fixture-upgrade"},()=>300000,overrides);assert.equal(waiting.claimed,0);
  await owner.query("UPDATE sessions.history_model_work SET lease_until=clock_timestamp()-interval '1 second' WHERE work_id=$1",[oldTicket.workId]);
  const restarted=await api({...env,BOB_COSTAS_MODEL:"fixture-upgrade"},()=>300000,overrides);assert.equal(restarted.completed,1);
  const jobs=(await owner.query("SELECT work_id,status,last_error_code FROM sessions.history_model_work WHERE config->>'model'='fixture-upgrade'")).rows;
  assert.equal(jobs.find(x=>x.work_id===oldTicket.workId).status,"canceled");assert.equal(jobs.find(x=>x.work_id!==oldTicket.workId).status,"complete");
  check("actual packaged discovery restarts expired old-generation pending work without adopting its nonce/frontier");
 }

 let wrongPool:pg.Pool|undefined;
 await assert.rejects(api(env,()=>300000,{...overrides,pool:()=>{wrongPool=new pg.Pool({host:"127.0.0.1",port,user:"postgres",database:"postgres",connectionTimeoutMillis:1000,max:1});jobPools.push(wrongPool);return wrongPool}}),{code:"model_database_role"});
 assert(jobPools.every(p=>(p as any).ending===true));check("actual owner connection refuses before provider and closes both pools");
 proof.rssMaxKiB=process.resourceUsage().maxRSS;proof.ok=true;
} catch(error){proof.ok=false;proof.error=String(error);throw error}
finally{if(provider)await new Promise<void>(r=>provider!.close(()=>r()));await Promise.all(pools.map(p=>p.end()));for(const c of containers.reverse())try{docker(["rm","-f",c])}catch{};writeFileSync(resolve(out,"proof.json"),JSON.stringify(proof,null,2)+"\n")}
