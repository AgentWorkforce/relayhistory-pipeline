/** Bounded metadata-only source discovery. Durable continuation is the Lambda
 * async event cursor, acknowledged only after this page's enqueue RPCs succeed. */
import type {ArtifactIdentity} from "../session-read-api/canonical-artifact.ts";
import {LEGACY_ANALYSIS_OWNERSHIP_SQL,type ReadonlyAnalysisSource} from "./analysis-snapshot.ts";
export type AnalysisReconcileCursor=[string,string,string];
export function checkedAnalysisCursor(value:unknown):AnalysisReconcileCursor|undefined{if(value===undefined)return undefined;if(!Array.isArray(value)||value.length!==3||value.some(v=>typeof v!=="string"||!v||v.length>512))throw new Error("analysis reconcile: invalid cursor");return value as AnalysisReconcileCursor}
type Key={org_id:string;workspace_id:string;session_id:string};
export async function reconcileAnalysisPage(deps:{source:ReadonlyAnalysisSource;enqueue:(identity:ArtifactIdentity)=>Promise<unknown>},options:{cursor?:AnalysisReconcileCursor;pageSize?:number;maxMs?:number;now?:()=>number}={}){
 const pageSize=options.pageSize??50;if(!Number.isInteger(pageSize)||pageSize<1||pageSize>100)throw new Error("analysis reconcile: invalid page size");const cursor=checkedAnalysisCursor(options.cursor);
 const now=options.now??Date.now,maxMs=options.maxMs??240000;if(!Number.isFinite(maxMs)||maxMs<1||maxMs>240000)throw new Error("analysis reconcile: invalid time budget");const stop=now()+maxMs;
 return deps.source.snapshot(async db=>{const {rows}=await db.query<Key>(`WITH candidates AS(
 SELECT org_id,workspace_id,session_id FROM sessions.session_analysis_requests
 UNION SELECT org_id,coalesce(workspace_id,CASE WHEN jsonb_typeof(source->'evidenceWorkspaceIds')='array' THEN CASE WHEN jsonb_array_length(source->'evidenceWorkspaceIds')=1 THEN source->'evidenceWorkspaceIds'->>0 END END),session_id FROM sessions.session_analysis_jobs
 ) SELECT org_id,workspace_id,session_id FROM candidates WHERE org_id IS NOT NULL AND workspace_id IS NOT NULL AND session_id IS NOT NULL
 AND length(org_id) BETWEEN 1 AND 512 AND btrim(org_id)<>'' AND length(workspace_id) BETWEEN 1 AND 512 AND btrim(workspace_id)<>'' AND length(session_id) BETWEEN 1 AND 512 AND btrim(session_id)<>''
 AND($1::text IS NULL OR(org_id COLLATE "C",workspace_id COLLATE "C",session_id COLLATE "C")>($1::text COLLATE "C",$2::text COLLATE "C",$3::text COLLATE "C"))
 ORDER BY org_id COLLATE "C",workspace_id COLLATE "C",session_id COLLATE "C" LIMIT $4`,[...(cursor??[null,null,null]),pageSize+1]);
 let scanned=0,accepted=0,unavailable=0,invalidScope=0;
 let last:Key|undefined;
 for(const k of rows.slice(0,pageSize)){
  if(now()>=stop)break;
  let scope:ArtifactIdentity|undefined;
  if([k.org_id,k.workspace_id,k.session_id].every(v=>v&&v.length<=512)){
   const evidence=(await db.query<{workspace_id:string|null;source:string|null}>(LEGACY_ANALYSIS_OWNERSHIP_SQL,[k.org_id,k.session_id])).rows;
   const providers=new Set(evidence.map(r=>r.source).filter((v):v is string=>v!==null)),source=[...providers][0];
   if(evidence.length&&evidence.every(r=>r.workspace_id===k.workspace_id)&&providers.size===1&&source&&/^[a-z0-9][a-z0-9._-]{0,63}$/u.test(source))scope={orgId:k.org_id,workspaceId:k.workspace_id,source,sessionId:k.session_id};
  }
  if(!scope)invalidScope++;
  else{
   const ack=await deps.enqueue(scope) as {accepted?:unknown};
   if(typeof ack?.accepted!=="boolean")throw new Error("analysis reconcile: invalid durable enqueue acknowledgement");
   if(ack.accepted)accepted++;else unavailable++;
  }
  // Advance only after this exact key is durably acknowledged or explicitly
  // classified unavailable/invalid. Failures replay the same input cursor.
  scanned++;last=k;
 }
 const hasMore=scanned<rows.length;
 if(hasMore&&!last)throw new Error("analysis reconcile: budget exhausted before progress");
 return {scanned,accepted,unavailable,invalidScope,nextCursor:hasMore&&last?[last.org_id,last.workspace_id,last.session_id] as AnalysisReconcileCursor:undefined,complete:!hasMore};
 });
}
