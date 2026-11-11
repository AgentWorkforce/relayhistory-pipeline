import { expect, test } from "bun:test";
import { Hono } from "hono";
import { registerSessionEraseRoutes, sessionEraseConfig } from "../../session-ingest/session-erase.ts";
const secret="synthetic-route-secret-0123456789abcdef";
const path="/internal/v1/session-erase/organizations/o/workspaces/w/sources/claude/sessions/s";
test("service auth and validation precede SQL; absent receipt is 404 and provider errors are static retryable503",async()=>{
 const app=new Hono(),lines:Record<string,unknown>[]=[];
 let writes=0,reads=0;
 registerSessionEraseRoutes(app,sessionEraseConfig({SESSION_ERASE_API_ENABLED:"1",SESSION_ERASE_API_SECRET:secret}),l=>lines.push(l),{
  async erase(){writes++;throw Error("private provider credential and transcript");},async receipt(){reads++;return null;},
 });
 const post=(body:string,authorized=true)=>app.request(path,{method:"POST",headers:{"content-type":"application/json",...(authorized?{authorization:`Bearer ${secret}`}:{})},body});
 const body=JSON.stringify({requestId:"r",actor:{kind:"user",id:"operator"}});
 expect((await post(body,false)).status).toBe(401);
 expect((await post("{}")).status).toBe(400);
 expect(writes).toBe(0);
 const missing=await app.request(path,{headers:{authorization:`Bearer ${secret}`}});
 expect([missing.status,await missing.json()]).toEqual([404,{error:"erasure_not_found",feature:"session_erase"}]);
 const failed=await post(body);
 expect([failed.status,failed.headers.get("retry-after"),await failed.json()]).toEqual([503,"60",{error:"unavailable",feature:"session_erase"}]);
 expect([writes,reads]).toEqual([1,1]);
 expect(JSON.stringify(lines)).not.toContain("private provider");expect(JSON.stringify(lines)).not.toContain(secret);
});
