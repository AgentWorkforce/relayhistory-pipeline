import {expect,test} from "bun:test";
import {database,pointer,KEY,hoopDb,read,code} from "./harness.ts";
import {recordBrief,type BriefText} from "../store.ts";
import {TENANT_SETTINGS_SQL} from "../../session-read-api/liveness.ts";
const ROLE=read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
const text:BriefText={title:"Actual native title",description:"Actual model description.",state:"completed",scrubRuleset:"relayhistory-scrub",scrubVersion:1,fingerprint:"f".repeat(64),promptVersion:"session-summary-v3",engineVersion:"session-brief-v3",model:"gpt-5.6-luna",messageCount:2,shortenedMessages:0,inputTokens:10,outputTokens:5,durationMs:3,generatedAt:new Date("2026-11-09T00:00:00Z")};
const logical="01a1bd4d-ab36-70cf-9580-642a1096361f";
async function withDb(run:(pg:Awaited<ReturnType<typeof database>>)=>Promise<void>){const pg=await database();try{await pg.exec(ROLE);await pointer(pg);await run(pg);}finally{await pg.close();}}
async function summary(pg:Awaited<ReturnType<typeof database>>,sid=KEY.sessionId,tenant=[KEY.orgId,KEY.workspaceId],workspace=KEY.workspaceId){
 await pg.exec("SET ROLE bulls96_read_api; BEGIN READ ONLY");try{await pg.query(TENANT_SETTINGS_SQL,tenant);const r=await pg.query("SELECT * FROM sessions.session_brief_summary_for($1,$2,$3)",[workspace,KEY.harness,sid]);await pg.exec("COMMIT");return r.rows;}catch(e){await pg.exec("ROLLBACK");throw e;}finally{await pg.exec("RESET ROLE");}
}
test("fresh native model output reads through tenant-pinned read role; canonical aliases and revoked rows are fenced",async()=>withDb(async pg=>{
 await recordBrief(hoopDb(pg),KEY,{"m-1":1},{status:"ready",text});
 expect((await summary(pg))[0]).toMatchObject({description:text.description,model:text.model,session_id:KEY.sessionId});
 await pg.query("UPDATE sessions.session_index SET canonical_session_id=$1",[logical]);
 expect(await summary(pg)).toEqual([]);
 expect((await summary(pg,logical))[0]).toMatchObject({session_id:logical,description:text.description});
 await pg.exec("UPDATE sessions.session_index SET revision=2");expect(await summary(pg,logical)).toEqual([]);
 await recordBrief(hoopDb(pg),KEY,{"m-1":2},{status:"ready",text});expect(await summary(pg,logical)).toHaveLength(1);
 await pg.exec("UPDATE sessions.session_index SET deleted_at=now()");expect(await summary(pg,logical)).toEqual([]);
}),60000);
test("pending, failed, missing and backfilled output never fabricate a native summary",async()=>withDb(async pg=>{
 expect(await summary(pg)).toEqual([]);
 await recordBrief(hoopDb(pg),KEY,{"m-1":1},{status:"ready",text});
 await pg.exec("UPDATE sessions.session_briefs SET status='failed'");expect(await summary(pg)).toEqual([]);
 await pg.exec("UPDATE sessions.session_briefs SET status='ready',engine_version='neon-backfill:test',legacy_job_id='job-1'");expect(await summary(pg)).toEqual([]);
}),60000);
test("read function ACL and role binding reject foreign/invalid callers without direct table grants; up/down/up",async()=>withDb(async pg=>{
 await recordBrief(hoopDb(pg),KEY,{"m-1":1},{status:"ready",text});
 expect(await summary(pg,KEY.sessionId,["foreign",KEY.workspaceId])).toEqual([]);
 expect(await code(summary(pg,KEY.sessionId,[KEY.orgId,"foreign"]))).toBe("42501");
 expect(await code(summary(pg,""))).toBe("22023");
 const acl=await pg.query<{role:string;allowed:boolean}>("SELECT rolname AS role,has_function_privilege(oid,'sessions.session_brief_summary_for(text,text,text)','EXECUTE') AS allowed FROM pg_roles WHERE rolname IN ('bulls96_read_api','anon','authenticated','service_role','search_consumer_runtime')");
 for(const r of acl.rows)expect(r.allowed).toBe(r.role==='bulls96_read_api');
 expect((await pg.query<{allowed:boolean}>("SELECT has_table_privilege('bulls96_read_api','sessions.session_briefs','SELECT') AS allowed")).rows[0]!.allowed).toBe(false);
 await pg.exec(read("supabase/bootstrap/session_read_api_role_verify.sql").split("\n").filter(l=>!l.startsWith("\\")).join("\n"));
 expect(await code(pg.query("SELECT * FROM sessions.session_brief_summary_for($1,$2,$3)",[KEY.workspaceId,KEY.harness,KEY.sessionId]))).toBe("42501");
 await pg.exec(read("supabase/rollbacks/20261105044030_session_brief_summary_read.down.sql"));
 expect((await pg.query<{n:number}>("SELECT count(*)::int n FROM sessions.session_briefs")).rows[0]!.n).toBe(1);
 await pg.exec(read("supabase/migrations/20261105044030_session_brief_summary_read.sql"));expect(await summary(pg)).toHaveLength(1);
}),60000);

test("multi-physical canonical group is unsupported instead of falsely fresh",async()=>withDb(async pg=>{
 await recordBrief(hoopDb(pg),KEY,{"m-1":1},{status:"ready",text});
 await pointer(pg,{key:{...KEY,sessionId:"second"}});
 await pg.query("UPDATE sessions.session_index SET canonical_session_id=$1",[logical]);
 expect(await code(summary(pg,logical))).toBe("0A000");
 await pg.exec("UPDATE sessions.session_index SET deleted_at=now() WHERE session_id='second'");
 expect(await summary(pg,logical)).toHaveLength(1);
}),60000);
