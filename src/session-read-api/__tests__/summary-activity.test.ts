import {expect,test} from "bun:test";
import {createReadApi,AuthError,type VerifiedIdentity} from "../app.ts";
import {BriefSummaryError,type BriefSummary,type SummaryActivityRow} from "../brief-summary.ts";
import {memoryLiveness} from "./support.ts";
const id:VerifiedIdentity={userId:"u",orgId:"o",workspaceId:"w",scopes:["rth:read"]};
const PATH="/v1/sessions/summaries/activity?workspace=w&since=2026-11-01T00:00:00.000Z&until=2026-11-04T23:59:59.999Z";
function api(opts:{rows?:SummaryActivityRow[]|null;error?:unknown;allowed?:boolean;configured?:boolean|"read-only";identity?:VerifiedIdentity}={}){
 const calls:unknown[]=[];
 const briefSummary:BriefSummary={async read(){throw new Error("unexpected per-session read");},
  async activity(...args){calls.push(args);if(opts.error)throw opts.error;return opts.rows===undefined?[{day:new Date("2026-11-02T00:00:00Z"),total:"3"},{day:"2026-11-04",total:1}]:opts.rows;}};
 const configured=opts.configured===false?{}:opts.configured==="read-only"?{briefSummary:{read:briefSummary.read}}:{briefSummary};
 const app=createReadApi({verify:async token=>{if(token!=="good")throw new AuthError(401,"invalid_token","invalid");return opts.identity??id;},ch:{database:"x"} as never,liveness:memoryLiveness().liveness,completeSinceMs:0,directoryWorkspaces:opts.allowed===false?new Set():new Set(["w"]),...configured});
 return {calls,get:(path=PATH,token:string|null="good")=>app.request(path,{headers:token?{authorization:`Bearer ${token}`}:{}})};
}
test("one zero-filled entry per UTC day, oldest first, counts only, attested",async()=>{
 const a=api(),r=await a.get();
 expect(r.status).toBe(200);
 expect(r.headers.get("x-relayhistory-workspace-id")).toBe("w");
 const body=await r.json();
 expect(body).toEqual({since:"2026-11-01T00:00:00.000Z",until:"2026-11-04T23:59:59.999Z",days:[
  {date:"2026-11-01",total:0},{date:"2026-11-02",total:3},{date:"2026-11-03",total:0},{date:"2026-11-04",total:1}]});
 // No ids, titles, sources or text: only since/until/days and {date,total}.
 expect(Object.keys(body).sort()).toEqual(["days","since","until"]);
 for(const d of body.days)expect(Object.keys(d).sort()).toEqual(["date","total"]);
 // Scoped to the token's org and workspace, with the parsed window.
 expect(a.calls).toEqual([["o","w",new Date("2026-11-01T00:00:00.000Z"),new Date("2026-11-04T23:59:59.999Z")]]);
 // A window with nothing generated is all zero days.
 expect((await (await api({rows:[]}).get()).json()).days).toEqual(["01","02","03","04"].map(d=>({date:`2026-11-${d}`,total:0})));
});
test("auth, workspace match, scope and canary precede the read",async()=>{
 for(const [options,path,token,status] of [
  [{},undefined,null,401],[{},undefined,"bad",401],
  [{identity:{...id,scopes:["rth:sync"]}},undefined,"good",403],
  [{},PATH.replace("workspace=w","workspace=other"),"good",403],
  [{},PATH.replace("workspace=w","workspace=w&workspace=w"),"good",400],
  [{allowed:false},undefined,"good",501],
 ] as const){const a=api(options),r=await a.get(path,token);expect(r.status).toBe(status);expect(a.calls).toHaveLength(0);}
});
test("since/until validation matches /v1/sessions/activity",async()=>{
 const base="/v1/sessions/summaries/activity?workspace=w";
 for(const q of ["","&since=2026-11-01T00:00:00Z","&until=2026-11-01T00:00:00Z","&since=nope&until=2026-11-01T00:00:00Z",
  "&since=2026-11-02T00:00:00Z&until=2026-11-01T00:00:00Z","&since=2026-01-01T00:00:00Z&until=2026-11-01T00:00:00Z"]){
  const a=api(),r=await a.get(base+q);expect(r.status).toBe(400);expect(a.calls).toHaveLength(0);
 }
 // 92 UTC days is the maximum span; 93 is refused.
 expect((await api({rows:[]}).get(`${base}&since=2026-08-01T00:00:00Z&until=2026-10-31T23:59:59Z`)).status).toBe(200);
 expect((await api().get(`${base}&since=2026-08-01T00:00:00Z&until=2026-11-01T00:00:00Z`)).status).toBe(400);
 // Unexpected filters are not silently ignored.
 const a=api();expect((await a.get(`${PATH}&source=codex`)).status).toBe(501);expect(a.calls).toHaveLength(0);
});
test("unconfigured backends, missing migration and bad rows never fabricate counts",async()=>{
 expect((await api({configured:false}).get()).status).toBe(501);
 expect((await api({configured:"read-only"}).get()).status).toBe(501);
 expect((await api({rows:null}).get()).status).toBe(501);
 expect((await api({error:new BriefSummaryError("refused")}).get()).status).toBe(503);
 for(const rows of [[{day:"2026-10-01",total:1}],[{day:"2026-11-02",total:-1}],[{day:"2026-11-02",total:"x"}]])expect((await api({rows}).get()).status).toBe(503);
});
