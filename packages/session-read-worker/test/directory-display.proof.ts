import {expect,inject,it} from "vitest";
import {canonicalHandle} from "../src/canonical.ts";
import type {ArtifactReadDeps} from "../../../src/session-read-api/canonical-artifact-reader.ts";
const fixture=()=>inject("artifactFixtureUrl");
async function setup(){
 const ack:any=await(await fetch(fixture()+"/claim")).json(),proof:any=await(await fetch(fixture()+"/proof")).json();
 let claims=0;
 const deps:ArtifactReadDeps={signal:new AbortController().signal,publication:async(identity)=>{
  expect(identity).toEqual(ack.identity);claims++;return ack;
 },read:async(key)=>{
  const res=await fetch(fixture()+"/object?"+new URLSearchParams({key}));
  if(!res.ok||!res.body)throw Error("fixture missing");
  return res.body;
 }};
 const token="header."+btoa(JSON.stringify({role:"session_directory_search_worker",app_metadata:{org_id:ack.identity.orgId,workspace_ids:[ack.identity.workspaceId]}}))+".signature";
 const req=new Request("https://read.invalid/v1/canonical/sessions/"+encodeURIComponent(ack.identity.sessionId)+"?"+new URLSearchParams({workspace:ack.identity.workspaceId,source:ack.identity.source,table:"session_directory_event_values",view:"raw",limit:"3"}),{headers:{authorization:"Bearer "+token}});
 return {ack,proof,deps,req,claims:()=>claims};
}
const forbidden=()=>{throw Error("raw/Neon network forbidden")};
it("actual HTTP Worker serves one compact display page from actual exporter/publication, with no raw transcript reads",async()=>{
 const x=await setup();await fetch(fixture()+"/reset");
 const response=await canonicalHandle(x.req,{CANONICAL_READ_ENABLED:"true",CANONICAL_READ_MODE:"pages"} as any,forbidden as any,undefined,x.deps);
 expect(response.status).toBe(200);
 const body:any=await response.json();
 expect(body.rows).toEqual([{table:"session_directory_event_values",row:x.proof.expected}]);
 expect(body.rows[0].row.event.ts).toBe("2020-01-01T00:00:00.123456Z");
 expect(body.rows[0].row.event.machine_id).toBe("machine:0");
 expect(body.nextCursor).toBeNull();expect(x.claims()).toBe(2);
 const stats:any=await(await fetch(fixture()+"/stats")).json();
 expect(stats.requests).toBe(2);expect(stats.totalBytes).toBeLessThan(16384);
 expect(x.proof.sourceContentChars).toBeGreaterThan(2500000);
});
it("source erasure after S3 hydration refuses the complete HTTP response",async()=>{
 const x=await setup();let calls=0;
 x.deps.publication=async()=>++calls===1?x.ack:{...x.ack,current:false};
 const response=await canonicalHandle(x.req,{CANONICAL_READ_ENABLED:"true",CANONICAL_READ_MODE:"pages"} as any,forbidden as any,undefined,x.deps);
 expect(response.status).toBe(503);expect(calls).toBe(2);
});
it("authenticated absence reads no S3 objects and never invents display values",async()=>{
 const x=await setup();await fetch(fixture()+"/reset");
 x.deps.publication=async()=>({version:1,identity:x.ack.identity,exists:false,current:false,source:x.ack.source});
 const response=await canonicalHandle(x.req,{CANONICAL_READ_ENABLED:"true",CANONICAL_READ_MODE:"pages"} as any,forbidden as any,undefined,x.deps);
 expect(response.status).toBe(200);
 const body:any=await response.json();expect(body.rows).toEqual([]);expect(body.exists).toBe(false);
 const stats:any=await(await fetch(fixture()+"/stats")).json();expect(stats.requests).toBe(0);
});
