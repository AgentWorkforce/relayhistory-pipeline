/** Real620/630 metadata + official export/fold/masked graph + encrypted objects.
 * Provider is an explicit offline contract fixture, NOT live model quality/spend.
 * Actual nonowner LOGIN/MinIO/packaged job proof is separate. */
import{test,expect}from"bun:test";
import{mkdtempSync}from"node:fs";import{tmpdir}from"node:os";import{join}from"node:path";
import{randomUUID}from"node:crypto";
import{replayDatabase,exportReplayFixture,createFsStore,REPLAY_FIXTURE_IDENTITY as identity}from"../../session-pipeline/__tests__/replay-fixture.ts";
import{generation}from"../../hoopdb/migrate.ts";
import{captureHistorySource,readHistorySummary}from"../../shared/history-source-authority.ts";
import{sqlHistoryMaskStore,updateHistoryMask}from"../../shared/history-mask.ts";
import{historyRecordContentSha256}from"../../shared/history-visibility.ts";
import{produceCanonicalReplay}from"../../session-pipeline/replay-publication.ts";
import{runFullModelWorkTick}from"../full-model-tick.ts";
import{readCurrentFullModelSummary}from"../full-model-summary-reader.ts";
import{readFullModelSummary}from"../full-model-artifacts.ts";
import{masterKeyring,parseMasterKeys}from"../../shared/keyring.ts";
import type{RelayhistoryStreamRecord}from"../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
test("actual complete masked graph creates durable work; all content precedes both encrypted summaries",async()=>{
 const x=await replayDatabase(),store=createFsStore({root:mkdtempSync(join(tmpdir(),"model-integration-"))});
 try{
 const gen=await generation(),authority=randomUUID(),scope={orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:identity.sessionId};
 const base="2026-11-01T10:00:00.000000Z",records:RelayhistoryStreamRecord[]=[];
 for(let i=0;i<31;i++)records.push({kind:"convergence_event",origin_id:"machine:machine:0",record_id:"native-"+i,revision:"1",accepted_seq:String(i+1),operation:"upsert",received_at:base,payload:{source:identity.source,session_id:identity.sessionId,machine_id:"machine:0",user_id:"user_replay",event_id:"native-"+i,kind:"session_event",type:"assistant",ts:base,ingested_at:base,actor_name:"fixture",actor_role:"assistant",content:i===23?"long all-content ".repeat(160000):"message-"+i,model:"fixture",tool_calls:i===23?Array.from({length:10001},(_,n)=>({id:n,args:{name:"tool",surrogate:"\ud800"}})):[],record:{payload:{message_id:"real-native-"+i,arbitrary:["kept",i]}}}});
 const visible:RelayhistoryStreamRecord={kind:"session_event",origin_id:"current-origin",record_id:"visible-native",revision:"1",accepted_seq:"9007199254740993",operation:"upsert",received_at:base,payload:{text:"VISIBLE-MASKED-EVIDENCE",role:"assistant",ts_ms:Date.parse(base),message_id:"visible-message"}};
 const hidden:RelayhistoryStreamRecord={...visible,record_id:"hidden-native",payload:{...visible.payload,text:"MOVED-PRIVATE-MUST-NOT-MODEL"}};
 records.push(visible,hidden);
 await exportReplayFixture(store,x.db,0,1,records);
 // Exact role boundary for mask registration and later job IO.
 const role=(name:string)=>({query:async<T>(q:string,p?:unknown[])=>{
  await x.pg.exec("BEGIN;SET LOCAL ROLE "+name);try{const r=await x.db.query<T>(q,p);await x.pg.exec("COMMIT");return r}catch(e){await x.pg.exec("ROLLBACK");throw e}
 }});
 const ingest=role("bulls96_session_ingest"),modelDb=role("bulls96_session_model");
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
 expect(graph.published).toBe(true);
 expect((await x.db.query("SELECT * FROM sessions.history_model_wakeups")).rows).toHaveLength(1);
 const inputs:string[]=[];let modelCalls=0;
 const model={complete:async(req:any)=>{
  modelCalls++;inputs.push(req.input);
  return{value:req.schemaName==="session_full_summary"?{short:"Fixture completed summary",long:"Fixture full evidence summary including message30 and large row",state:"done",stateReason:"fixture only"}:{notes:"Bounded evidence from every supplied fragment"},model:"fixture-model-v1",tokensIn:100,tokensOut:20,providerRequestId:null};
 }};
 const keyring=await masterKeyring(parseMasterKeys("fixture:"+"e".repeat(64)));
 const bounded={...modelDb,queryBounded:async<T>(q:string,p:unknown[]|undefined,context:any)=>{context.signal.throwIfAborted();return modelDb.query<T>(q,p)}};
 let completed=0;
 for(let tick=0;tick<10&&!completed;tick++){
  const result=await runFullModelWorkTick({enabled:true,schemaGeneration:gen,db:bounded,heartbeatDb:bounded,store,keyring,config:{model:"fixture-model",url:"https://api.openai.com/v1/responses",timeoutMs:1000,maxOutputTokens:2048},model,remainingMs:()=>300000,maxSteps:64});
  completed+=result.completed;
 }
 expect(completed).toBe(1);expect(modelCalls).toBeGreaterThan(10);
 expect(inputs.some(v=>v.includes("message-30"))).toBe(true);
 expect(inputs.some(v=>v.includes("VISIBLE-MASKED-EVIDENCE"))).toBe(true);
 expect(inputs.every(v=>!v.includes("MOVED-PRIVATE-MUST-NOT-MODEL"))).toBe(true);
 expect((await x.db.query("SELECT * FROM sessions.history_model_wakeups")).rows).toHaveLength(0);
 expect((await x.db.query<any>("SELECT status FROM sessions.history_model_work")).rows[0]?.status).toBe("complete");
 for(const kind of["short","long"]as const){
  const pointer=await readHistorySummary(ingest,history,kind);expect(pointer?.status).toBe("ready");if(!pointer)throw Error("pointer");
  const record=await readFullModelSummary({store,keyring,captured:history,pointer,signal:AbortSignal.timeout(5000)});
  expect(record?.summary).toContain("Fixture");
 }
 expect((await x.db.query("SELECT * FROM sessions.ingest_history_states")).rows).toHaveLength(0);
 const read=await readCurrentFullModelSummary({db:ingest,store,keyring,scope:identity,schemaGeneration:gen,kind:"short",signal:AbortSignal.timeout(5000)});
 expect(read.status).toBe("ready");
 // Revoke this exact logical source DURING object IO. Ciphertext remains
 // physically readable; the final source/pointer fence must withhold its text.
 let invalidated=false;
 const raced=new Proxy(store,{get(target,key){
  if(key==="getObject")return async(...args:any[])=>{
   const object=await(target.getObject as any)(...args);
   if(!invalidated){invalidated=true;await x.db.query("INSERT INTO sessions.ingest_lazy_native_slots(key_sha256,key_material,org_id,workspace_id,origin_id,record_id,source_identity_sha256,control_epoch,initialized_witness_sha256)VALUES(repeat('f',64),'[]',$1,$2,'current-origin','visible-native',repeat('c',64),repeat('d',64),repeat('e',64))",[identity.orgId,identity.workspaceId]);}
   return object;
  };
  const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value;
 }});
 const changed=await readCurrentFullModelSummary({db:ingest,store:raced,keyring,scope:identity,schemaGeneration:gen,kind:"short",signal:AbortSignal.timeout(5000)});
 expect(invalidated).toBe(true);expect(changed).toEqual({status:"unavailable"});
 }finally{await x.pg.close()}
},60000);
