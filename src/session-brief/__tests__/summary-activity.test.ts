import {expect,test} from "bun:test";
import {database,pointer,KEY,hoopDb,read,code} from "./harness.ts";
import {recordBrief,type BriefText} from "../store.ts";
import {TENANT_SETTINGS_SQL} from "../../session-read-api/liveness.ts";
import {sqlBriefSummary} from "../../session-read-api/brief-summary.ts";
const ROLE=read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
const text=(generatedAt:string):BriefText=>({title:"Actual native title",description:"Actual model description.",state:"completed",scrubRuleset:"relayhistory-scrub",scrubVersion:1,fingerprint:"f".repeat(64),promptVersion:"session-summary-v3",engineVersion:"session-brief-v3",model:"gpt-5.6-luna",messageCount:2,shortenedMessages:0,inputTokens:10,outputTokens:5,durationMs:3,generatedAt:new Date(generatedAt)});
const OTHER={...KEY,workspaceId:"ws-b"};
const SINCE="2026-11-01T00:00:00Z",UNTIL="2026-11-03T23:59:59.999Z";
type Pg=Awaited<ReturnType<typeof database>>;
async function withDb(run:(pg:Pg)=>Promise<void>){const pg=await database();try{await pg.exec(ROLE);await run(pg);}finally{await pg.close();}}
async function brief(pg:Pg,key:typeof KEY,at:string){await pointer(pg,{key});await recordBrief(hoopDb(pg),key,{"m-1":1},{status:"ready",text:text(at)});}
async function activity(pg:Pg,tenant=[KEY.orgId,KEY.workspaceId],workspace=KEY.workspaceId,since=SINCE,until=UNTIL){
 await pg.exec("SET ROLE bulls96_read_api; BEGIN READ ONLY");
 try{await pg.query(TENANT_SETTINGS_SQL,tenant);const r=await pg.query<{day:Date;total:number}>("SELECT day::text AS day,total::int AS total FROM sessions.session_summary_activity_for($1,$2,$3)",[workspace,since,until]);await pg.exec("COMMIT");return r.rows;}
 catch(e){await pg.exec("ROLLBACK");throw e;}finally{await pg.exec("RESET ROLE");}
}
test("counts ready native summaries per UTC day for the pinned workspace only",async()=>withDb(async pg=>{
 await brief(pg,KEY,"2026-11-01T10:00:00Z");
 await brief(pg,{...KEY,sessionId:"s-2"},"2026-11-01T23:59:59Z");
 await brief(pg,{...KEY,sessionId:"s-3"},"2026-11-03T00:00:00Z");
 await brief(pg,{...KEY,sessionId:"s-out"},"2026-11-04T00:00:00Z");
 // Another workspace in the same org: never counted for ws-a.
 await brief(pg,OTHER,"2026-11-01T12:00:00Z");
 await brief(pg,{...OTHER,sessionId:"s-2"},"2026-11-02T12:00:00Z");
 expect(await activity(pg)).toEqual([{day:"2026-11-01",total:2},{day:"2026-11-03",total:1}]);
 expect(await activity(pg,[KEY.orgId,OTHER.workspaceId],OTHER.workspaceId)).toEqual([{day:"2026-11-01",total:1},{day:"2026-11-02",total:1}]);
 // A token pinned to one workspace cannot read another's counts.
 expect(await code(activity(pg,[KEY.orgId,KEY.workspaceId],OTHER.workspaceId))).toBe("42501");
 expect(await activity(pg,["foreign",KEY.workspaceId])).toEqual([]);
}),60000);
test("pending, failed, stale, deleted and backfilled briefs are not counted, as the per-session summary shows none",async()=>withDb(async pg=>{
 await brief(pg,KEY,"2026-11-02T10:00:00Z");
 expect(await activity(pg)).toEqual([{day:"2026-11-02",total:1}]);
 await pg.exec("UPDATE sessions.session_index SET revision=2");expect(await activity(pg)).toEqual([]);
 await pg.exec("UPDATE sessions.session_index SET revision=1");expect(await activity(pg)).toHaveLength(1);
 await pg.exec("UPDATE sessions.session_briefs SET status='failed'");expect(await activity(pg)).toEqual([]);
 await pg.exec("UPDATE sessions.session_briefs SET status='ready',engine_version='neon-backfill:test',legacy_job_id='job-1'");expect(await activity(pg)).toEqual([]);
 await pg.exec("UPDATE sessions.session_briefs SET engine_version='session-brief-v3',legacy_job_id=NULL");expect(await activity(pg)).toHaveLength(1);
 await pg.exec("UPDATE sessions.session_index SET deleted_at=now()");expect(await activity(pg)).toEqual([]);
}),60000);
test("invalid windows refuse; ACL is read-role only with no table grant; the service reader returns rows",async()=>withDb(async pg=>{
 expect(await code(activity(pg,undefined,undefined,UNTIL,SINCE))).toBe("22023");
 expect(await code(activity(pg,undefined,undefined,"2025-01-01T00:00:00Z",UNTIL))).toBe("22023");
 const acl=await pg.query<{role:string;allowed:boolean}>("SELECT rolname AS role,has_function_privilege(oid,'sessions.session_summary_activity_for(text,timestamptz,timestamptz)','EXECUTE') AS allowed FROM pg_roles WHERE rolname IN ('bulls96_read_api','anon','authenticated','service_role','search_consumer_runtime')");
 expect(acl.rows.length).toBeGreaterThan(0);
 for(const r of acl.rows)expect(r.allowed).toBe(r.role==='bulls96_read_api');
 await brief(pg,KEY,"2026-11-02T10:00:00Z");
 const reader=sqlBriefSummary(async(orgId,workspaceId,work)=>{
  await pg.exec("SET ROLE bulls96_read_api; BEGIN READ ONLY");
  try{await pg.query(TENANT_SETTINGS_SQL,[orgId,workspaceId]);const out=await work({query:(t:string,p?:unknown[])=>pg.query(t,p as never)} as never);await pg.exec("COMMIT");return out;}
  catch(e){await pg.exec("ROLLBACK");throw e;}finally{await pg.exec("RESET ROLE");}
 });
 const rows=await reader.activity!(KEY.orgId,KEY.workspaceId,new Date(SINCE),new Date(UNTIL));
 expect(rows!.map(r=>[String(r.day instanceof Date?r.day.toISOString().slice(0,10):r.day).slice(0,10),Number(r.total)])).toEqual([["2026-11-02",1]]);
}),60000);

test("a logical session with several live physical aliases is not counted, as the per-session summary refuses it",async()=>withDb(async pg=>{
 const logical="01a1bd4d-ab36-70cf-9580-642a1096361f";
 await brief(pg,KEY,"2026-11-02T10:00:00Z");
 await pg.query("UPDATE sessions.session_index SET canonical_session_id=$1",[logical]);
 expect(await activity(pg)).toEqual([{day:"2026-11-02",total:1}]);
 await pointer(pg,{key:{...KEY,sessionId:"second"}});
 await pg.query("UPDATE sessions.session_index SET canonical_session_id=$1 WHERE session_id='second'",[logical]);
 expect(await activity(pg)).toEqual([]);
 await pg.exec("UPDATE sessions.session_index SET deleted_at=now() WHERE session_id='second'");
 expect(await activity(pg)).toEqual([{day:"2026-11-02",total:1}]);
}),60000);
