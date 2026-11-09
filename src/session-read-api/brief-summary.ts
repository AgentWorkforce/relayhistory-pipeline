/** A read-only native brief description. No model calls or generation enqueue. */
import type { TenantRunner } from "./liveness.ts";
export interface BriefSummaryRow {
 org_id:string; workspace_id:string; harness:string; session_id:string;
 description:string; model:string; generated_at:Date|string;
}
export interface BriefSummary {
 read(orgId:string, workspaceId:string, source:string, sessionId:string):Promise<BriefSummaryRow[]|null>;
}
export class BriefSummaryError extends Error {
 constructor(readonly reason:"invalid_argument"|"refused"|"unqualified"|"tenant_mismatch"|"parse") { super(`brief summary: ${reason}`); }
}
export const BRIEF_SUMMARY_SIGNATURE="sessions.session_brief_summary_for(text,text,text)";
export function sqlBriefSummary(run:TenantRunner):BriefSummary {
 return {async read(orgId,workspaceId,source,sessionId) {
  return run(orgId,workspaceId,async q=>{
   const installed=await q.query<{installed:boolean}>("SELECT to_regprocedure($1) IS NOT NULL AS installed",[BRIEF_SUMMARY_SIGNATURE]);
   if(!installed.rows[0]?.installed)return null;
   try { return (await q.query<BriefSummaryRow>("SELECT * FROM sessions.session_brief_summary_for($1,$2,$3)",[workspaceId,source,sessionId])).rows; }
   catch(e) {const code=(e as {code?:string})?.code;
    if(code==="22023")throw new BriefSummaryError("invalid_argument");
    if(code==="42501")throw new BriefSummaryError("refused");
    if(code==="0A000")throw new BriefSummaryError("unqualified");
    throw e;
   }
  });
 }};
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
