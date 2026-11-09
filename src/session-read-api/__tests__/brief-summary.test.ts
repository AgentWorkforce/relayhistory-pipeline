import {expect,test} from "bun:test";
import {createReadApi,AuthError,type VerifiedIdentity} from "../app.ts";
import {BriefSummaryError,type BriefSummary,type BriefSummaryRow} from "../brief-summary.ts";
import {memoryLiveness} from "./support.ts";
const id:VerifiedIdentity={userId:"u",orgId:"o",workspaceId:"w",scopes:["rth:read"]};
const row:BriefSummaryRow={org_id:"o",workspace_id:"w",harness:"codex",session_id:"s",description:"Actual model output.",model:"gpt-5.6-luna",generated_at:new Date("2026-11-09T00:00:00Z")};
function api(opts:{rows?:BriefSummaryRow[]|null;error?:unknown;allowed?:boolean;configured?:boolean;identity?:VerifiedIdentity}={}){
 const calls:unknown[]=[];const briefSummary:BriefSummary={async read(...args){calls.push(args);if(opts.error)throw opts.error;return opts.rows===undefined?[row]:opts.rows;}};
 const app=createReadApi({verify:async token=>{if(token!=="good")throw new AuthError(401,"invalid_token","invalid");return opts.identity??id;},ch:{database:"x"} as never,liveness:memoryLiveness().liveness,completeSinceMs:0,directoryWorkspaces:opts.allowed===false?new Set():new Set(["w"]),...(opts.configured===false?{}:{briefSummary})});
 return {calls,get:(path="/v1/sessions/s/summary?workspace=w&source=codex&kind=short",token:string|null="good")=>app.request(path,{headers:token?{authorization:`Bearer ${token}`}:{}})};
}
test("native summary serializes actual model text/provenance, no-store; missing is explicitly empty",async()=>{
 const a=api(),r=await a.get();expect(r.status).toBe(200);expect(r.headers.get("cache-control")).toBe("private, no-store");
 expect(await r.json()).toEqual({summary:{status:"ready",kind:"short",summary:row.description,model:row.model,builtAt:"2026-11-09T00:00:00.000Z",state:null}});
 expect(a.calls).toEqual([["o","w","codex","s"]]);
 expect(await (await api({rows:[]}).get()).json()).toEqual({summary:{status:"empty",kind:"short",state:null}});
});
test("auth, workspace, scope and canary precede the read",async()=>{
 for(const [options,path,token,status] of [
  [{},undefined,null,401],[{},undefined,"bad",401],
  [{identity:{...id,scopes:["rth:sync"]}},undefined,"good",403],
  [{},"/v1/sessions/s/summary?workspace=foreign&source=codex","good",403],
  [{allowed:false},undefined,"good",501],
 ] as const){const a=api(options),r=await a.get(path,token);expect(r.status).toBe(status);expect(a.calls).toHaveLength(0);}
});
test("unsupported kind, repeated/missing source and unexpected filters never read or fabricate empty",async()=>{
 for(const suffix of ["source=codex&kind=long","source=codex&source=claude","kind=short","source=codex&q=secret","source=Codex"]){const a=api();expect((await a.get(`/v1/sessions/s/summary?workspace=w&${suffix}`)).status).toBeGreaterThanOrEqual(400);expect(a.calls).toHaveLength(0);}
});
test("missing schema, unqualified source and malformed/foreign rows are failures, never empty200",async()=>{
 expect((await api({configured:false}).get()).status).toBe(501);
 expect((await api({rows:null}).get()).status).toBe(501);
 expect((await api({error:new BriefSummaryError("unqualified")}).get()).status).toBe(501);
 for(const rows of [[{...row,org_id:"other"}],[{...row,workspace_id:"other"}],[{...row,session_id:"other"}],[{...row,description:""}],[{...row,generated_at:"invalid"}],[row,row]])expect((await api({rows}).get()).status).toBeGreaterThanOrEqual(500);
});
