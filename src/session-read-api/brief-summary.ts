/** A read-only native brief description. No model calls or generation enqueue. */
import type { TenantRunner } from "./liveness.ts";
export interface BriefSummaryRow {
 org_id:string; workspace_id:string; harness:string; session_id:string;
 description:string; model:string; generated_at:Date|string;
 /** v2 (T130) only: the served brief's revision and per-machine source revisions. Absent from v1. */
 brief_revision?:number|string|bigint|null; source_revisions?:Record<string,number>|null;
 /** v2 only: opaque identity of the served brief, passed back to membership to detect a switch. */
 brief_binding?:string|null;
}
export interface BriefSummary {
 read(orgId:string, workspaceId:string, source:string, sessionId:string):Promise<BriefSummaryRow[]|null>;
 /** Ready native summaries per UTC day (counts only); null = 056000 not applied. Absent = 501. */
 activity?(orgId:string, workspaceId:string, since:Date, until:Date):Promise<SummaryActivityRow[]|null>;
 /** T130: one record's membership in the served brief; null = 062510 not applied. Absent = 501. */
 membership?(orgId:string, workspaceId:string, source:string, sessionId:string, recordId:string, binding?:string):Promise<BriefMembershipRow[]|null>;
}
export interface BriefMembershipRow {
 org_id:string; workspace_id:string; harness:string; session_id:string;
 membership:string; brief_revision:number|string|bigint|null; source_revisions:Record<string,number>|null; brief_binding:string|null;
}
export const BRIEF_MEMBERSHIP_SIGNATURE="sessions.session_brief_membership_for(text,text,text,text,text)";
const MEMBERSHIP=["member","not_member","unknown","changed"] as const;
const BINDING=/^[0-9a-f]{64}$/;
/** The served brief's opaque binding from a v2 row, or null (v1 / no brief). Malformed is a parse failure. */
export function briefSummaryBinding(rows:readonly BriefSummaryRow[]):string|null {
 if(rows.length!==1)return null;
 const b=rows[0]!.brief_binding;
 if(b===undefined||b===null)return null;
 if(typeof b!=="string"||!BINDING.test(b))throw new BriefSummaryError("parse");
 return b;
}
/** The API shape of one membership answer; the record id is never echoed back. */
export function briefMembershipResponse(rows:readonly BriefMembershipRow[], scope:{orgId:string;workspaceId:string;source:string;sessionId:string}) {
 if(rows.length!==1)throw new BriefSummaryError("parse");
 const row=rows[0]!;
 if(row.org_id!==scope.orgId||row.workspace_id!==scope.workspaceId)throw new BriefSummaryError("tenant_mismatch");
 if(row.harness!==scope.source||row.session_id!==scope.sessionId||!(MEMBERSHIP as readonly string[]).includes(row.membership))throw new BriefSummaryError("parse");
 const revision=row.brief_revision===null?null:briefSummaryRevision([{...row,description:"",model:"",generated_at:new Date(0),brief_revision:row.brief_revision}]);
 const binding=row.brief_binding??null;
 if(binding!==null&&(typeof binding!=="string"||!BINDING.test(binding)))throw new BriefSummaryError("parse");
 if((row.membership==="member"||row.membership==="not_member")&&(revision===null||binding===null))throw new BriefSummaryError("parse");
 return {status:row.membership as typeof MEMBERSHIP[number],revision,sourceRevisions:row.source_revisions??null,binding};
}
export interface SummaryActivityRow { day:Date|string; total:number|string|bigint; }
export class BriefSummaryError extends Error {
 constructor(readonly reason:"invalid_argument"|"refused"|"unqualified"|"tenant_mismatch"|"parse") { super(`brief summary: ${reason}`); }
}
export const BRIEF_SUMMARY_SIGNATURE="sessions.session_brief_summary_for(text,text,text)";
/** T130 Reader provenance: v1 plus the served revision. Used when installed; v1 otherwise. */
export const BRIEF_SUMMARY_V2_SIGNATURE="sessions.session_brief_summary_v2_for(text,text,text)";
export const SUMMARY_ACTIVITY_SIGNATURE="sessions.session_summary_activity_for(text,timestamptz,timestamptz)";
function mapSummaryError(e:unknown):never {
 const code=(e as {code?:string})?.code;
 if(code==="22023")throw new BriefSummaryError("invalid_argument");
 if(code==="42501")throw new BriefSummaryError("refused");
 if(code==="0A000")throw new BriefSummaryError("unqualified");
 throw e;
}
export function sqlBriefSummary(run:TenantRunner):BriefSummary {
 return {async read(orgId,workspaceId,source,sessionId) {
  return run(orgId,workspaceId,async q=>{
   const installed=await q.query<{v1:boolean;v2:boolean}>("SELECT to_regprocedure($1) IS NOT NULL AS v1, to_regprocedure($2) IS NOT NULL AS v2",[BRIEF_SUMMARY_SIGNATURE,BRIEF_SUMMARY_V2_SIGNATURE]);
   const v2=installed.rows[0]?.v2===true;
   if(!v2&&!installed.rows[0]?.v1)return null;
   try { return (await q.query<BriefSummaryRow>(v2?"SELECT * FROM sessions.session_brief_summary_v2_for($1,$2,$3)":"SELECT * FROM sessions.session_brief_summary_for($1,$2,$3)",[workspaceId,source,sessionId])).rows; }
   catch(e) {mapSummaryError(e);}
  });
 },
 async membership(orgId,workspaceId,source,sessionId,recordId,binding) {
  return run(orgId,workspaceId,async q=>{
   const installed=await q.query<{installed:boolean}>("SELECT to_regprocedure($1) IS NOT NULL AS installed",[BRIEF_MEMBERSHIP_SIGNATURE]);
   if(!installed.rows[0]?.installed)return null;
   try { return (await q.query<BriefMembershipRow>("SELECT * FROM sessions.session_brief_membership_for($1,$2,$3,$4,$5)",[workspaceId,source,sessionId,recordId,binding??null])).rows; }
   catch(e) {mapSummaryError(e);}
  });
 },
 async activity(orgId,workspaceId,since,until) {
  return run(orgId,workspaceId,async q=>{
   const installed=await q.query<{installed:boolean}>("SELECT to_regprocedure($1) IS NOT NULL AS installed",[SUMMARY_ACTIVITY_SIGNATURE]);
   if(!installed.rows[0]?.installed)return null;
   try { return (await q.query<SummaryActivityRow>("SELECT day, total FROM sessions.session_summary_activity_for($1,$2,$3)",[workspaceId,since.toISOString(),until.toISOString()])).rows; }
   catch(e) {mapSummaryError(e);}
  });
 }};
}
/** Every UTC day in [since, until], oldest first, zero-filled. Counts only: no ids or text. */
export function summaryActivityDays(rows:readonly SummaryActivityRow[], dates:readonly string[]):{date:string;total:number}[] {
 const byDate=new Map(dates.map(date=>[date,0]));
 for(const row of rows) {
  const date=row.day instanceof Date?row.day.toISOString().slice(0,10):String(row.day).slice(0,10);
  const total=Number(row.total);
  if(!byDate.has(date)||!Number.isSafeInteger(total)||total<0)throw new BriefSummaryError("parse");
  byDate.set(date,total);
 }
 return dates.map(date=>({date,total:byDate.get(date)!}));
}
export function briefSummaryResponse(rows:readonly BriefSummaryRow[], scope:{orgId:string;workspaceId:string;source:string;sessionId:string}) {
 if(rows.length===0)return {status:"empty" as const,kind:"short" as const,state:null};
 if(rows.length!==1)throw new BriefSummaryError("parse");
 const row=rows[0]!;
 if(row.org_id!==scope.orgId||row.workspace_id!==scope.workspaceId)throw new BriefSummaryError("tenant_mismatch");
 if(row.harness!==scope.source||row.session_id!==scope.sessionId)throw new BriefSummaryError("parse");
 const builtAt=row.generated_at instanceof Date?row.generated_at:new Date(row.generated_at);
 if(typeof row.description!=="string"||!row.description.trim()||[...row.description].length>320||typeof row.model!=="string"||!row.model.trim()||row.model.length>128||!Number.isFinite(builtAt.getTime()))throw new BriefSummaryError("parse");
 return {status:"ready" as const,kind:"short" as const,summary:row.description,model:row.model,builtAt:builtAt.toISOString(),state:null};
}
/**
 * The served brief's revision for provenance: a non-negative safe integer from the v2 read, or
 * null when the row came from v1 (no column). A present but malformed value is a parse failure.
 */
export function briefSummaryRevision(rows:readonly BriefSummaryRow[]):number|null {
 if(rows.length!==1)return null;
 const raw=rows[0]!.brief_revision;
 if(raw===undefined||raw===null)return null;
 const n=typeof raw==="bigint"?Number(raw):typeof raw==="string"&&/^\d+$/.test(raw)?Number(raw):raw;
 if(typeof n!=="number"||!Number.isSafeInteger(n)||n<0)throw new BriefSummaryError("parse");
 return n;
}
