import { test, expect } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { prepareBoundSummaryValue, putPreparedSummaryObject, readBoundSummaryValue } from "../summary-objects.ts";
import { initialFullModelCheckpoint, fullModelConfigHash, stepFullModelWork, validateFullModelCheckpoint, type FullModelStepPorts, type ModelSlice, type ModelSliceRef } from "../full-model-work.ts";

const source="a".repeat(64),config=fullModelConfigHash("fixture-model","hkdf1:k1");
async function ports(rows: any[]){
 const store=createFsStore({root:mkdtempSync(join(tmpdir(),"full-work-"))});
 const keyring=await masterKeyring(parseMasterKeys("k1:"+"b".repeat(64)));
 const ids={orgId:"org-fixture",workspaceId:"ws-fixture"};
 const observed: any[]=[];let calls=0,writes=0;
 const p:FullModelStepPorts={
  readRow:async ord=>rows[Number(ord)]??null,
  model:{complete:async req=>{
   calls++;const input=JSON.parse(req.input);
   if(input.protocol==="bob-canonical-input-v1")observed.push(...input.fragments);
   return {value:req.schemaName==="session_full_summary"
    ?{short:"Goal and observed outcome",long:"Detailed results, failed attempts and remaining work",state:"unknown",stateReason:"No explicit completion evidence"}
    :{notes:"Evidence from bounded input unit "+calls},model:"fixture-model",tokensIn:100,tokensOut:20,providerRequestId:null};
  }},
  writeSlice:async(value,range)=>{
   writes++;
   const key="fixture/source/registered/"+randomUUID()+".bin";
   const prepared=await prepareBoundSummaryValue({keyring,ids,key,binding:source,value:value as unknown as Record<string,unknown>});
   await putPreparedSummaryObject({store,prepared});
   return {key,sha256:prepared.sha256,bytes:prepared.bytes,keyId:prepared.keyId,...range};
  },
  readSlice:async ref=>{
   const v=await readBoundSummaryValue({store,keyring,ids,...ref,binding:source});
   if(!v)throw Error("slice_integrity");
   return v as unknown as ModelSlice;
  }
 };
 return {p,observed,get calls(){return calls},get writes(){return writes}};
}
test("durable encrypted tree processes every row and long arbitrary fields before BOTH summaries",async()=>{
 const rows=Array.from({length:37},(_,i)=>({table:"convergence_events",row:{id:"e"+i,content:i===23?"LONG".repeat(150000):"prompt-"+i,tool_calls:i===23?Array.from({length:900},(_,j)=>({id:j,args:["\ud800","\\",j]})):[]}}));
 const fixture=await ports(rows);
 let c=initialFullModelCheckpoint(source,config),maxRefs=0,maxCheckpoint=0,done=false;
 for(let i=0;i<300;i++){
  // A new process can reconstruct only the metadata checkpoint + encrypted refs.
  c=JSON.parse(JSON.stringify(c));
  const before=JSON.stringify(c),beforeCalls=fixture.calls;
  const out=await stepFullModelWork({ports:fixture.p,checkpoint:c,sourceBinding:source,configSha256:config,signal:AbortSignal.timeout(10000)});
  expect(JSON.stringify(c)).toBe(before);
  expect(fixture.calls-beforeCalls).toBeLessThanOrEqual(1);
  c=out.next;validateFullModelCheckpoint(c,source,config);
  maxRefs=Math.max(maxRefs,c.frontier.length+(c.carry?1:0));
  maxCheckpoint=Math.max(maxCheckpoint,Buffer.byteLength(JSON.stringify(c)));
  if(out.kind==="complete"){expect(out.conclusion?.short).toBeTruthy();expect(out.conclusion?.long).toContain("remaining work");done=true;break;}
 }
 expect(done).toBe(true);expect(BigInt(c.units)).toBeGreaterThan(10n);
 expect(maxRefs).toBeLessThan(10);expect(maxCheckpoint).toBeLessThan(12000);
 const fragments=fixture.observed;
 for(let i=0;i<rows.length;i++){
  const parts=fragments.filter(f=>f.rowOrdinal===String(i)).sort((a,b)=>a.utf16Offset-b.utf16Offset);
  expect(parts.length).toBeGreaterThan(0);
  const json=parts.map(p=>p.jsonFragment).join("");
  expect(JSON.parse(json)).toEqual(rows[i]);
  expect(parts.every(p=>p.rowSha256===createHash("sha256").update(json).digest("hex"))).toBe(true);
 }
 expect(fragments.some(f=>f.rowOrdinal==="36")).toBe(true);
});
test("crash after immutable slice write never advances caller cursor or falsely completes",async()=>{
 const fixture=await ports([{table:"conversation_turns",row:{content:"real content"}}]);
 const c=initialFullModelCheckpoint(source,config),original=JSON.stringify(c);
 const write=fixture.p.writeSlice;
 fixture.p.writeSlice=async(...args)=>{await write(...args);throw Error("crash_before_checkpoint");};
 await expect(stepFullModelWork({ports:fixture.p,checkpoint:c,sourceBinding:source,configSha256:config,signal:AbortSignal.timeout(10000)})).rejects.toThrow("crash_before_checkpoint");
 expect(JSON.stringify(c)).toBe(original);
 expect(fixture.writes).toBe(1);
 fixture.p.writeSlice=write;
 const retry=await stepFullModelWork({ports:fixture.p,checkpoint:c,sourceBinding:source,configSha256:config,signal:AbortSignal.timeout(10000)});
 expect(retry.kind).toBe("checkpoint");expect(retry.next.units).toBe("1");expect(fixture.writes).toBe(2);
});
test("missing coverage, changed source/config and wrong encrypted range refuse before model",async()=>{
 const fixture=await ports([]),c=initialFullModelCheckpoint(source,config);
 for(const changed of [{...c,sourceBinding:"c".repeat(64)},{...c,configSha256:"c".repeat(64)},{...c,units:"1"}])
  await expect(stepFullModelWork({ports:fixture.p,checkpoint:changed,sourceBinding:source,configSha256:config,signal:AbortSignal.timeout(10000)})).rejects.toThrow();
 expect(fixture.calls).toBe(0);
 const fake:ModelSliceRef={key:"fixture/k",keyId:"hkdf1:k1",sha256:"f".repeat(64),bytes:200,first:"0",count:"1",level:0};
 const bad={...c,units:"1",inputComplete:true,frontier:[fake]};
 fixture.p.readSlice=async()=>({protocol:"bob-notes-v1",sourceBinding:"d".repeat(64),first:"0",count:"1",notes:"wrong source"});
 await expect(stepFullModelWork({ports:fixture.p,checkpoint:bad,sourceBinding:source,configSha256:config,signal:AbortSignal.timeout(10000)})).rejects.toThrow("model_slice_binding");
 expect(fixture.calls).toBe(0);
});
test("verified empty source completes without a fabricated model summary",async()=>{
 const fixture=await ports([]),c=initialFullModelCheckpoint(source,config);
 const first=await stepFullModelWork({ports:fixture.p,checkpoint:c,sourceBinding:source,configSha256:config,signal:AbortSignal.timeout(10000)});
 const done=await stepFullModelWork({ports:fixture.p,checkpoint:first.next,sourceBinding:source,configSha256:config,signal:AbortSignal.timeout(10000)});
 expect(done.kind).toBe("complete");if(done.kind==="complete")expect(done.conclusion).toBeNull();expect(fixture.calls).toBe(0);
});
