/**
 * The read-only scrubbed long summary (migration 20261105060000), served as
 * GET /v1/sessions/:sessionId/summary?kind=long only when wired
 * (READ_API_LONG_SUMMARY_ENABLED exactly "true"). No model calls or enqueue.
 * Errors reuse BriefSummaryError so the route maps them exactly like kind=short.
 */
import type { TenantRunner } from "./liveness.ts";
import { BriefSummaryError, mapSummaryError } from "./brief-summary.ts";
export interface LongSummaryRow {
 org_id:string; workspace_id:string; harness:string; session_id:string;
 long_summary:string; source:string; generated_at:Date|string;
}
export interface LongSummary {
 /** null = 060000 not applied (the route answers 501 summary_not_migrated). */
 read(orgId:string, workspaceId:string, source:string, sessionId:string):Promise<LongSummaryRow[]|null>;
}
export const LONG_SUMMARY_SIGNATURE="sessions.session_long_summary_for(text,text,text)";
/** Mirrors the database bound (char_length BETWEEN 1 AND 8000). */
export const LONG_SUMMARY_MAX_CHARS=8000;
export function sqlLongSummary(run:TenantRunner):LongSummary {
 return {async read(orgId,workspaceId,source,sessionId) {
  return run(orgId,workspaceId,async q=>{
   const installed=await q.query<{installed:boolean}>("SELECT to_regprocedure($1) IS NOT NULL AS installed",[LONG_SUMMARY_SIGNATURE]);
   if(!installed.rows[0]?.installed)return null;
   try { return (await q.query<LongSummaryRow>("SELECT * FROM sessions.session_long_summary_for($1,$2,$3)",[workspaceId,source,sessionId])).rows; }
   catch(e) {mapSummaryError(e);}
  });
 }};
}
const SOURCE=/^[a-z0-9][a-z0-9._/-]{0,63}$/;
export function longSummaryResponse(rows:readonly LongSummaryRow[], scope:{orgId:string;workspaceId:string;source:string;sessionId:string}) {
 if(rows.length===0)return {status:"empty" as const,kind:"long" as const,state:null};
 if(rows.length!==1)throw new BriefSummaryError("parse");
 const row=rows[0]!;
 if(row.org_id!==scope.orgId||row.workspace_id!==scope.workspaceId)throw new BriefSummaryError("tenant_mismatch");
 if(row.harness!==scope.source||row.session_id!==scope.sessionId)throw new BriefSummaryError("parse");
 const builtAt=row.generated_at instanceof Date?row.generated_at:new Date(row.generated_at);
 if(typeof row.long_summary!=="string"||!row.long_summary.trim()||[...row.long_summary].length>LONG_SUMMARY_MAX_CHARS||typeof row.source!=="string"||!SOURCE.test(row.source)||!Number.isFinite(builtAt.getTime()))throw new BriefSummaryError("parse");
 return {status:"ready" as const,kind:"long" as const,summary:row.long_summary,model:row.source,builtAt:builtAt.toISOString(),state:null};
}
