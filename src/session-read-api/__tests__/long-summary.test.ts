/**
 * GET /v1/sessions/:sessionId/summary?kind=long (long-summary.ts, migration 20261105060000):
 * 501 summary_kind unless `longSummary` is wired; wired, it reads with the token's own
 * org/workspace and maps errors exactly like kind=short. No database: recording fakes.
 */
import {expect,test} from "bun:test";
import {createReadApi,AuthError,type VerifiedIdentity} from "../app.ts";
import {BriefSummaryError,type BriefSummary,type BriefSummaryRow} from "../brief-summary.ts";
import {LONG_SUMMARY_SIGNATURE,longSummaryResponse,sqlLongSummary,type LongSummary,type LongSummaryRow} from "../long-summary.ts";
import {memoryLiveness} from "./support.ts";
const id:VerifiedIdentity={userId:"u",orgId:"o",workspaceId:"w",scopes:["rth:read"]};
const row:LongSummaryRow={org_id:"o",workspace_id:"w",harness:"codex",session_id:"s",long_summary:"A longer scrubbed account of the session.",source:"bob-costas/extractive/v1",generated_at:new Date("2026-11-09T00:00:00Z")};
const shortRow:BriefSummaryRow={org_id:"o",workspace_id:"w",harness:"codex",session_id:"s",description:"Short.",model:"m",generated_at:new Date("2026-11-09T00:00:00Z")};
function api(opts:{rows?:LongSummaryRow[]|null;error?:unknown;long?:boolean;brief?:boolean;identity?:VerifiedIdentity;allowed?:boolean}={}){
 const calls:unknown[]=[],briefCalls:unknown[]=[];
 const longSummary:LongSummary={async read(...args){calls.push(args);if(opts.error)throw opts.error;return opts.rows===undefined?[row]:opts.rows;}};
 const briefSummary:BriefSummary={async read(...args){briefCalls.push(args);return [shortRow];}};
 const app=createReadApi({verify:async token=>{if(token!=="good")throw new AuthError(401,"invalid_token","invalid");return opts.identity??id;},ch:{database:"x"} as never,liveness:memoryLiveness().liveness,completeSinceMs:0,
  directoryWorkspaces:opts.allowed===false?new Set():new Set(["w"]),...(opts.brief===false?{}:{briefSummary}),...(opts.long===false?{}:{longSummary})});
 return {calls,briefCalls,get:(path="/v1/sessions/s/summary?workspace=w&source=codex&kind=long",token:string|null="good")=>app.request(path,{headers:token?{authorization:`Bearer ${token}`}:{}})};
}
const reason=async(r:Response)=>((await r.json()) as {error:{reason?:string}}).error.reason;

test("unwired: kind=long is 501 summary_kind and nothing is read (today's behaviour)",async()=>{
 const a=api({long:false}),r=await a.get();
 expect(r.status).toBe(501);expect(await reason(r)).toBe("summary_kind");
 expect(a.calls).toHaveLength(0);expect(a.briefCalls).toHaveLength(0);
 // Neither summary wired: unchanged 501 summary_not_configured.
 const none=api({long:false,brief:false}),n=await none.get();
 expect(n.status).toBe(501);expect(await reason(n)).toBe("summary_not_configured");
});

test("wired: ready text with provenance, no-store, read with the token's own org/workspace",async()=>{
 const a=api(),r=await a.get();
 expect(r.status).toBe(200);expect(r.headers.get("cache-control")).toBe("private, no-store");
 expect(await r.json()).toEqual({summary:{status:"ready",kind:"long",summary:row.long_summary,model:row.source,builtAt:"2026-11-09T00:00:00.000Z",state:null}});
 expect(a.calls).toEqual([["o","w","codex","s"]]);expect(a.briefCalls).toHaveLength(0);
 // A missing row is the same explicit empty as kind=short.
 const e=await api({rows:[]}).get();expect(e.status).toBe(200);
 expect(await e.json()).toEqual({summary:{status:"empty",kind:"long",state:null}});
 // kind=short is untouched by wiring long.
 const s=api(),sr=await s.get("/v1/sessions/s/summary?workspace=w&source=codex&kind=short");
 expect(sr.status).toBe(200);expect(s.calls).toHaveLength(0);expect(s.briefCalls).toEqual([["o","w","codex","s"]]);
 // Only long wired: kind=short stays 501 summary_not_configured.
 const only=api({brief:false}),or=await only.get("/v1/sessions/s/summary?workspace=w&source=codex");
 expect(or.status).toBe(501);expect(await reason(or)).toBe("summary_not_configured");
});

test("cross-workspace and cross-org never read or serve another tenant",async()=>{
 for(const [options,path,status] of [
  [{},"/v1/sessions/s/summary?workspace=foreign&source=codex&kind=long",403],
  [{allowed:false},undefined,501],
  [{identity:{...id,scopes:["rth:sync"]}},undefined,403],
 ] as const){const a=api(options),r=await a.get(path);expect(r.status).toBe(status);expect(a.calls).toHaveLength(0);}
 for(const token of [null,"bad"]){const a=api();expect((await a.get(undefined,token)).status).toBe(401);expect(a.calls).toHaveLength(0);}
 // A row from another org or workspace (a broken backend) is a failure, never served.
 for(const rows of [[{...row,org_id:"other"}],[{...row,workspace_id:"other"}]]){
  const r=await api({rows}).get();expect(r.status).toBeGreaterThanOrEqual(400);
  expect(JSON.stringify(await r.json())).not.toContain(row.long_summary);
 }
});

test("unknown params, bad keys and backend errors map like kind=short",async()=>{
 for(const suffix of ["source=codex&kind=long&q=secret","source=codex&kind=long&limit=1","source=codex&source=claude&kind=long","kind=long","source=Codex&kind=long","source=codex&kind=medium"]){
  const a=api(),r=await a.get(`/v1/sessions/s/summary?workspace=w&${suffix}`);expect(r.status).toBeGreaterThanOrEqual(400);expect(a.calls).toHaveLength(0);
 }
 const p=await api().get("/v1/sessions/s/summary?workspace=w&source=codex&kind=long&q=x");expect(p.status).toBe(501);expect(await reason(p)).toBe("param_q");
 const m=await api({rows:null}).get();expect(m.status).toBe(501);expect(await reason(m)).toBe("summary_not_migrated");
 expect((await api({error:new BriefSummaryError("unqualified")}).get()).status).toBe(501);
 expect((await api({error:new BriefSummaryError("invalid_argument")}).get()).status).toBe(400);
 expect((await api({error:new BriefSummaryError("refused")}).get()).status).toBe(503);
 for(const rows of [[{...row,session_id:"other"}],[{...row,long_summary:""}],[{...row,long_summary:"x".repeat(8001)}],[{...row,source:"Bad Source"}],[{...row,generated_at:"invalid"}],[row,row]])
  expect((await api({rows}).get()).status).toBeGreaterThanOrEqual(500);
});

test("longSummaryResponse accepts exactly the 8000 code point bound",()=>{
 const scope={orgId:"o",workspaceId:"w",source:"codex",sessionId:"s"};
 expect(longSummaryResponse([{...row,long_summary:"\u{1F600}".repeat(8000)}],scope).status).toBe("ready");
 expect(()=>longSummaryResponse([{...row,long_summary:"\u{1F600}".repeat(8001)}],scope)).toThrow(BriefSummaryError);
});

test("sqlLongSummary passes the identity's org/workspace to the tenant runner and maps SQLSTATEs",async()=>{
 const seen:unknown[]=[];let fail:string|undefined;
 const run=(async(org:string,ws:string,fn:(q:{query:(t:string,p?:unknown[])=>Promise<{rows:unknown[]}>})=>unknown)=>{
  seen.push(["tenant",org,ws]);
  return fn({query:async(text,params)=>{seen.push([text,params]);
   if(text.startsWith("SELECT to_regprocedure"))return {rows:[{installed:true}]};
   if(fail)throw Object.assign(new Error("x"),{code:fail});return {rows:[row]};}});
 }) as never;
 const reader=sqlLongSummary(run);
 expect(await reader.read("o","w","codex","s")).toEqual([row]);
 expect(seen).toEqual([["tenant","o","w"],["SELECT to_regprocedure($1) IS NOT NULL AS installed",[LONG_SUMMARY_SIGNATURE]],["SELECT * FROM sessions.session_long_summary_for($1,$2,$3)",["w","codex","s"]]]);
 for(const [code,mapped] of [["22023","invalid_argument"],["42501","refused"],["0A000","unqualified"]] as const){
  fail=code;const e=await reader.read("o","w","codex","s").then(()=>null,(x:unknown)=>x);
  expect(e).toBeInstanceOf(BriefSummaryError);expect((e as BriefSummaryError).reason).toBe(mapped);
 }
});
