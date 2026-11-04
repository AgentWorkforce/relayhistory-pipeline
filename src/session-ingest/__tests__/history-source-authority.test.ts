import{test,expect}from"bun:test";
import{captureHistorySource,captureHistorySourceReference,readHistoryReplayDescriptor,withHistorySourceAuthority,historyDerivedKey,registerHistoryDerivedObject,publishHistorySummary,publishHistoryState}from"../../shared/history-source-authority.ts";
const scope={orgId:"org",workspaceId:"ws",source:"claude",sessionId:"session"},GEN="a".repeat(64),ID="12345678-1234-1234-1234-123456789abc",HASH="b".repeat(64);
const descriptor=()=>({version:1,required:true,ready:true,authorityId:ID,maskSha256:HASH,recordCount:"1",derivedAnchorKey:"96bulls/org/upload-generations/g="+ID+"/v1/ws=ws/m=history_scope/h=claude/d=2026/11/01/s=session/publications/"+ID+"/manifest.json"});
const capture=()=>({exists:true,blocked:false,identity:scope,source:{epoch:"9007199254740993",fingerprint:HASH},snapshot:{epoch:"9007199254740993",history:descriptor(),pointers:[]}});
const winner=()=>({originId:"o",recordId:"native",kind:"history",revision:"1",acceptedSeq:"9007199254740994",operation:"upsert",userId:"user",source:"claude",sessionId:"session",contentSha256:HASH,lineage:"1"});
function fixture(mutate?:(q:string,p:any,result:any,calls:number)=>void){let captures=0;const calls:any[]=[];const db:any={query:async(q:string,params:any[])=>{const p=JSON.parse(params[0]);calls.push({q,p});let r:any;if(q.includes("session_history_capture")){captures++;r={result:capture()};}else if(q.includes("mask_page"))r={result:{authorityId:ID,total:"1",next:null,records:[{ordinal:"0",recordId:"native",winner:winner()}]}};else if(q.includes("source_lock"))r={result:{current:true}};else if(q.includes("register_object"))r={result:{registered:true}};else r={result:{applied:true}};mutate?.(q,p,r,captures);return{rows:[r]}}};return{db,calls}}
test("captures explicit complete mask and retains large epoch/seat strings; missingmask neverbecomesempty",async()=>{
 const f=fixture(),c=await captureHistorySource(f.db,scope,{schemaGeneration:GEN});expect(c.source.epoch).toBe("9007199254740993");expect(c.mask.winners.get("native")?.acceptedSeq).toBe("9007199254740994");expect(f.calls.filter(r=>r.q.includes("session_history_capture")).length).toBe(2);
 const missing=fixture((q,p,r)=>{if(q.includes("session_history_capture"))delete r.result.snapshot.history});await expect(captureHistorySource(missing.db,scope,{schemaGeneration:GEN})).rejects.toThrow("history_source_unavailable");
});
test("mask terminalcoverage, owner/bodyshape and sourcechange refuse before consumers can use inputs",async()=>{
 for(const mutation of[(r:any)=>r.result.total="2",(r:any)=>r.result.records=[],(r:any)=>r.result.records[0].winner.contentSha256="bad"]){const f=fixture((q,p,r)=>{if(q.includes("mask_page"))mutation(r)});await expect(captureHistorySource(f.db,scope,{schemaGeneration:GEN})).rejects.toThrow("history_");}
 const changed=fixture((q,p,r,n)=>{if(q.includes("session_history_capture")&&n===2)r.result.source.fingerprint="c".repeat(64)});await expect(captureHistorySource(changed.db,scope,{schemaGeneration:GEN})).rejects.toThrow("history_source_changed");
});
test("finalCAS suppresses stale callback; exact summary/state metadata and objectintents retain original source",async()=>{
 const f=fixture(),c=await captureHistorySource(f.db,scope,{schemaGeneration:GEN});let writes=0;const stale=fixture((q,p,r)=>{if(q.includes("source_lock"))r.result.current=false});expect(await withHistorySourceAuthority(async f=>f(stale.db),c,async()=>{writes++})).toEqual({current:false});expect(writes).toBe(0);
 const key=historyDerivedKey(c,"bob-slice",ID);await registerHistoryDerivedObject(f.db,c,{key,sha256:HASH,bytes:100});
 const r=await withHistorySourceAuthority(async work=>work(f.db),c,async tx=>{await publishHistorySummary(tx,c,{kind:"short",status:"ready",objectKey:key,objectSha256:HASH,objectBytes:100,keyId:"fixture",model:"model",builtAt:"2026-11-04T00:00:00Z"});await publishHistoryState(tx,c,"active","2026-11-04T00:00:00Z");return"done"});expect(r).toEqual({current:true,value:"done"});
 for(const call of f.calls.filter(x=>!x.q.includes("session_history_capture"))){expect(call.p.source).toEqual(c.source);expect(call.p.authorityId).toBe(c.authorityId);expect(call.p.maskSha256).toBe(c.maskSha256);expect(call.p.generation).toBe(GEN)}
});

test("compact continuation reference does not hydrate winner map and descriptor refuses a different source",async()=>{
 const f=fixture(),r=await captureHistorySourceReference(f.db,scope,{schemaGeneration:GEN});
 expect("mask" in r).toBe(false);expect(f.calls.length).toBe(1);
 expect(f.calls[0].p).toEqual({generation:GEN,identity:scope});
 const graph={version:1,identity:{sessionId:scope.sessionId,source:scope.source,workspaceId:scope.workspaceId,orgId:scope.orgId},source:r.source,generation:GEN,prefix:"fixture/",index:{key:"fixture/index.json",sha256:HASH,bytes:12}};
 const db:any={query:async()=>({rows:[{result:{current:true,publication:graph}}]})};
 expect(await readHistoryReplayDescriptor(db,r)).toEqual(graph);
 graph.source={epoch:"2",fingerprint:HASH};
 await expect(readHistoryReplayDescriptor(db,r)).rejects.toThrow("history_replay_descriptor");
 const stale:any={query:async()=>({rows:[{result:{current:false,publication:null}}]})};
 expect(await readHistoryReplayDescriptor(stale,r)).toBeNull();
});
