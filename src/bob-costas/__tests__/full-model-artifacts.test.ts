import{test,expect}from"bun:test";
import{mkdtempSync}from"node:fs";import{tmpdir}from"node:os";import{join}from"node:path";
import{createFsStore}from"../../../packages/object-store/src/fs.ts";
import{masterKeyring,parseMasterKeys}from"../../shared/keyring.ts";
import type{HistorySourceCapture}from"../../shared/history-source-authority.ts";
import{modelArtifactPorts,prepareFullModelResults}from"../full-model-artifacts.ts";
import{fullModelSourceBinding}from"../full-model-input.ts";
import{initialFullModelCheckpoint}from"../full-model-work.ts";
const ID="12345678-1234-1234-1234-123456789abc",H="a".repeat(64),G="b".repeat(64),scope={orgId:"org",workspaceId:"ws",source:"claude",sessionId:"session"};
const captured:HistorySourceCapture={identity:scope,source:{epoch:"9007199254740993",fingerprint:H},schemaGeneration:G,authorityId:ID,maskSha256:H,recordCount:"1",derivedAnchorKey:"96bulls/org/upload-generations/g="+ID+"/v1/ws=ws/m=history_scope/h=claude/d=2026/11/01/s=session/publications/"+ID+"/manifest.json",mask:{scope,source:{epoch:"9007199254740993",fingerprint:H},winners:new Map()}};
async function fixture(){
 const raw=createFsStore({root:mkdtempSync(join(tmpdir(),"model-artifacts-"))}),intents=new Map<string,any>(),events:string[]=[];
 let accepted=true;
 const db:any={query:async(q:string,p:any[])=>{expect(q).toContain("session_history_register_object");const a=JSON.parse(p[0]);events.push("intent");if(accepted)intents.set(a.key,a);return{rows:[{result:{registered:accepted}}]}}};
 const store=new Proxy(raw,{get(t,k){if(k==="put")return async(...args:any[])=>{events.push("put");const i=intents.get(args[0]);expect(i?.bytes).toBe(args[1].byteLength);expect(args[2]?.contentSha256).toBe(i.sha256);expect(args[2]?.ifNoneMatch).toBe(true);return(t.put as any)(...args)};const v=Reflect.get(t,k);return typeof v==="function"?v.bind(t):v;}});
 const keyring=await masterKeyring(parseMasterKeys("k1:"+"c".repeat(64)));
 const d={db,store,keyring,captured,workId:ID,configSha256:H};
 return{d,events,intents,deny(){accepted=false;}};
}
test("encrypted intermediate intent always precedes PUT and binds original captured source",async()=>{
 const f=await fixture(),ports=modelArtifactPorts(f.d);
 const value={protocol:"bob-notes-v1" as const,sourceBinding:fullModelSourceBinding(captured),first:"0",count:"1",notes:"PRIVATE-MODEL-NOTES"};
 const ref=await ports.writeSlice(value,{first:"0",count:"1",level:0},AbortSignal.timeout(10000));
 expect(f.events).toEqual(["intent","put"]);expect(await ports.readSlice(ref,AbortSignal.timeout(10000))).toEqual(value);
 const intent=f.intents.get(ref.key);expect(intent.source).toEqual(captured.source);expect(intent.maskSha256).toBe(captured.maskSha256);
 const foreign=modelArtifactPorts({...f.d,captured:{...captured,source:{...captured.source,epoch:"9007199254740994"}}});
 await expect(foreign.readSlice(ref,AbortSignal.timeout(10000))).rejects.toThrow("model_slice_integrity");
 f.deny();await expect(ports.writeSlice(value,{first:"1",count:"1",level:0},AbortSignal.timeout(10000))).rejects.toThrow("history_object_registration");
 expect(f.events).toEqual(["intent","put","intent"]);
});
test("both encrypted summary pointers retain same large epoch, source and usage; text never enters SQL",async()=>{
 const f=await fixture(),checkpoint=initialFullModelCheckpoint(fullModelSourceBinding(captured),H);checkpoint.inputComplete=true;checkpoint.units="1";checkpoint.calls="1";
 const result=await prepareFullModelResults(f.d,{checkpoint,model:"fixture-model",builtAt:"2026-11-04T00:00:00Z",signal:AbortSignal.timeout(10000),conclusion:{short:"PRIVATE-SHORT",long:"PRIVATE-LONG",state:"done",stateReason:"observed complete"}});
 expect(result.pointers.map(p=>p.kind)).toEqual(["short","long"]);expect(result.state).toBe("done");expect(f.events).toEqual(["intent","put","intent","put"]);
 for(const p of result.pointers){expect(p.status).toBe("ready");expect(p.keyId).toBe("hkdf1:k1");expect(JSON.stringify(f.intents.get(p.objectKey!))).not.toContain("PRIVATE");}
});
test("empty result has no model text/object writes; uncompleted source refuses",async()=>{
 const f=await fixture(),checkpoint=initialFullModelCheckpoint(fullModelSourceBinding(captured),H);
 await expect(prepareFullModelResults(f.d,{checkpoint,model:"fixture-model",builtAt:"2026-11-04T00:00:00Z",signal:AbortSignal.timeout(10000),conclusion:null})).rejects.toThrow("model_result_binding");
 checkpoint.inputComplete=true;
 const r=await prepareFullModelResults(f.d,{checkpoint,model:"fixture-model",builtAt:"2026-11-04T00:00:00Z",signal:AbortSignal.timeout(10000),conclusion:null});
 expect(r.pointers.every(p=>p.status==="empty"&&p.objectKey===null)).toBe(true);expect(f.events).toEqual([]);
});
