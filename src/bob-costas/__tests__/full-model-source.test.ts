import{test,expect}from"bun:test";
import{mkdtempSync}from"node:fs";import{tmpdir}from"node:os";import{join}from"node:path";
import{createFsStore}from"../../../packages/object-store/src/fs.ts";
import{materializeCanonicalArtifacts}from"../../session-pipeline/replay-pages.ts";
import{openFullModelSource}from"../full-model-source.ts";
import type{HistorySourceReference}from"../../shared/history-source-authority.ts";
const H="a".repeat(64),G="b".repeat(64),ID="12345678-1234-1234-1234-123456789abc";
const identity={orgId:"org",workspaceId:"ws",source:"claude",sessionId:"session"};
const captured:HistorySourceReference={identity,source:{epoch:"9007199254740993",fingerprint:H},authorityId:ID,maskSha256:H,recordCount:"1",schemaGeneration:G,derivedAnchorKey:"96bulls/org/upload-generations/g="+ID+"/v1/ws=ws/m=history_scope/h=claude/d=2026/11/01/s=session/publications/"+ID+"/manifest.json"};
async function fixture(){
 const store=createFsStore({root:mkdtempSync(join(tmpdir(),"model-source-"))});
 const huge={table:"convergence_events",row:{source:identity.source,session_id:identity.sessionId,org_id:identity.orgId,workspace_id:identity.workspaceId,event_id:"native",machine_id:"m",kind:"session_event",ts:"2026-11-04T00:00:00.000000Z",content:"long".repeat(550000),tool_calls:Array.from({length:10001},(_,i)=>({id:i,args:{text:"\ud800",n:i}})),record:{payload:{message_id:"native-id",arbitrary:{afterTen:true,list:["full","native","payload"]}}}}};
 const legacy={table:"conversation_turns",row:{source:identity.source,session_id:identity.sessionId,workspace_id:identity.workspaceId,id:"9007199254740993",turn_index:1,legacy_stored_source:"actor-name",user_message:"legacy body",assistant_message:"legacy answer"}};
 const timing={table:"session_timing",row:{source:identity.source,session_id:identity.sessionId,duration_ms:1234}};
 const built=await materializeCanonicalArtifacts(store,{identity,source:captured.source,generation:H,prefix:"96bulls/org/fixture/current/",rows:[huge,legacy,timing],legacyCoverage:{version:1,tables:["conversation_turns","session_links","session_outcomes"]}});
 const publication={version:1 as const,identity,generation:H,source:captured.source,prefix:built.document.prefix,index:built.index};
 let ready=true,changed=false;
 const db:any={query:async(q:string,params:any[])=>{
  expect(q).toContain("session_history_replay_descriptor");
  const e=JSON.parse(params[0]);expect(e.source).toEqual(captured.source);expect(e.maskSha256).toBe(captured.maskSha256);expect(e.generation).toBe(G);
  return{rows:[{result:{current:!changed,publication:ready?publication:null}}]};
 }};
 return{store,db,huge,legacy,timing,publication,notReady(){ready=false},change(){changed=true}};
}
test("all raw graph fields and extra declared tables survive bounded ordinal model reads",async()=>{
 const f=await fixture(),signal=AbortSignal.timeout(20000);
 const reader=await openFullModelSource({...f,captured,signal});
 expect(reader.totalRows).toBe("3");
 expect(await reader.readRow("0",signal)).toEqual(f.huge);
 expect(await reader.readRow("1",signal)).toEqual(f.legacy);
 expect(await reader.readRow("2",signal)).toEqual(f.timing);
 expect(await reader.readRow("3",signal)).toBeNull();
 await expect(reader.readRow("4",signal)).rejects.toThrow("model_source_ordinal");
 f.change();await expect(reader.readRow("3",signal)).rejects.toThrow("model_source_changed");
});
test("unpublished graph refuses, never falls back to raw/unmasked source",async()=>{
 const f=await fixture();f.notReady();
 await expect(openFullModelSource({...f,captured,signal:AbortSignal.timeout(20000)})).rejects.toThrow("model_source_graph_pending");
});
