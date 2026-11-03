/** Readonly legacy snapshot. No read request uses this source connection.
 * Uses the exact v1 SQL projection as SELECT CTEs (0031); never rebuilds
 * public IDs, brief/result envelopes or status transitions in TypeScript. */
import pg,{type Pool} from "pg";
import type {SqlExecutor} from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import {ANALYSIS_V1_SELECT_SQL} from "./analysis-v1-select.ts";
import type {ArtifactIdentity} from "../session-read-api/canonical-artifact.ts";
import {ANALYSIS_MAX_HISTORY,type SessionAnalysisSnapshot} from "../session-read-api/session-analysis-artifact.ts";
export class AnalysisSnapshotError extends Error {constructor(public readonly code:string){super(code);this.name="AnalysisSnapshotError"}}
function refuse(code:string):never{throw new AnalysisSnapshotError(code)}
export type ReadonlyAnalysisSource={snapshot<T>(work:(db:SqlExecutor)=>Promise<T>):Promise<T>};
/** The real source adapter enforces isolation/read-only at the server, not by SQL
 * string inspection. Source credentials must separately be read-only. */
export function readonlyAnalysisSource(pool:Pool):ReadonlyAnalysisSource{return {async snapshot<T>(work:(db:SqlExecutor)=>Promise<T>){const c=await pool.connect();try{await c.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");await c.query("SET LOCAL statement_timeout='15s'");const result=await work({async query<R>(sql:string,params?:unknown[]){const value=await c.query(sql,params);return {rows:value.rows as R[]}}});await c.query("COMMIT");return result}catch(e){await c.query("ROLLBACK").catch(()=>{});throw e}finally{c.release()}}}}
export const LEGACY_ANALYSIS_OWNERSHIP_SQL=`SELECT DISTINCT workspace_id,source FROM (
 SELECT workspace_id,source FROM sessions.convergence_events WHERE org_id=$1 AND session_id=$2
 UNION ALL SELECT workspace_id,coalesce(nullif(source,''),metadata->>'nativeCli',metadata->>'source') source FROM sessions.conversation_turns WHERE org_id=$1 AND session_id=$2
 ) evidence`;
type RequestProof={id:string;workspace_id:string;analysis_job_id:string|null;brief_job_id:string|null;a_id:string|null;b_id:string|null;a_workspace_ids:unknown;b_workspace_ids:unknown;a_superseded:boolean};
const sameWorkspace=(v:unknown,workspace:string)=>Array.isArray(v)&&v.length===1&&v[0]===workspace;
export async function readLegacyAnalysisSnapshot(source:ReadonlyAnalysisSource,identity:ArtifactIdentity):Promise<SessionAnalysisSnapshot>{return source.snapshot(async db=>{
 const {rows:ownership}=await db.query<{workspace_id:string|null;source:string|null}>(LEGACY_ANALYSIS_OWNERSHIP_SQL,[identity.orgId,identity.sessionId]);
 const {rows}=await db.query<{snapshot_at:string;snapshot_id:string;payload:SessionAnalysisSnapshot["payload"]}>(`SELECT to_char(transaction_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') snapshot_at,encode(sha256(convert_to(pg_current_snapshot()::text,'UTF8')),'hex') snapshot_id,p.payload FROM (${ANALYSIS_V1_SELECT_SQL}) p`,[identity.orgId,identity.sessionId]);
 const value=rows[0];if(!value||!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/u.test(value.snapshot_at)||!value.snapshot_id)refuse("analysis_snapshot_invalid");
 const p=value.payload;if(p?.outcome==="not_found"){if(ownership.length)refuse("analysis_projection_invalid");return {snapshotAt:value.snapshot_at,snapshotId:value.snapshot_id,payload:p}}
 if(!ownership.length||ownership.some(r=>r.workspace_id!==identity.workspaceId))refuse("analysis_workspace_unverified");
 const providers=new Set(ownership.map(r=>r.source).filter((s):s is string=>s!==null));
 if(providers.size!==1||!providers.has(identity.source))refuse("analysis_source_mismatch");
 if(p?.outcome!=="ok"||p.sessionId!==identity.sessionId||!Array.isArray(p.requests)||!Array.isArray(p.analyses)||p.requests.length>ANALYSIS_MAX_HISTORY||p.analyses.length>ANALYSIS_MAX_HISTORY)refuse("analysis_projection_invalid");
 const ids=p.requests.map(r=>r.id);if(ids.some(id=>typeof id!=="string")||new Set(ids).size!==ids.length)refuse("analysis_projection_invalid");
 const {rows:proofs}=await db.query<RequestProof>(`SELECT r.id::text,r.workspace_id,r.analysis_job_id::text,r.brief_job_id::text,
 a.id::text a_id,b.id::text b_id,a.source->'evidenceWorkspaceIds' a_workspace_ids,b.source->'evidenceWorkspaceIds' b_workspace_ids,
 coalesce(a.status='failed' AND a.last_error='superseded' AND r.brief_job_id IS NOT NULL,false) a_superseded
 FROM sessions.session_analysis_requests r
 LEFT JOIN sessions.session_analysis_jobs a ON a.id=r.analysis_job_id AND a.org_id=r.org_id AND a.session_id=r.session_id
 LEFT JOIN sessions.session_analysis_jobs b ON b.id=r.brief_job_id AND b.org_id=r.org_id AND b.session_id=r.session_id
 WHERE r.org_id=$1 AND r.session_id=$2 AND r.id=ANY($3::uuid[])`,[identity.orgId,identity.sessionId,ids]);
 const byId=new Map(proofs.map(r=>[r.id,r]));
 // Filtering applies before exposing request history. The source function owns
 // its 50-row window and ordering; this preserves that selected subsequence.
 const requests=p.requests.filter(r=>byId.get(String(r.id))?.workspace_id===identity.workspaceId);
 const contributors=new Map<string,RequestProof[]>();for(const r of requests){if(r.analysisId!==null&&typeof r.analysisId!=="string")refuse("analysis_projection_invalid");if(typeof r.analysisId==="string"){const list=contributors.get(r.analysisId)??[];list.push(byId.get(String(r.id))!);contributors.set(r.analysisId,list)}}
 const safe=(r:RequestProof)=>{
  const analysisRequired=r.analysis_job_id!==null&&!r.a_superseded,briefRequired=r.brief_job_id!==null;
  return (analysisRequired||briefRequired)&&(!analysisRequired||(r.a_id===r.analysis_job_id&&sameWorkspace(r.a_workspace_ids,identity.workspaceId)))&&(!briefRequired||(r.b_id===r.brief_job_id&&sameWorkspace(r.b_workspace_ids,identity.workspaceId)));
 };
 const analysisIds=new Set<string>();const analyses=p.analyses.filter(a=>contributors.has(String(a.id))).map(a=>{if(typeof a.id!=="string"||analysisIds.has(a.id)||a.sessionId!==identity.sessionId)refuse("analysis_projection_invalid");analysisIds.add(a.id);return contributors.get(a.id)!.every(safe)?a:{...a,result:null,status:"failed",error:"workspace_scope_unverified"}});
 if(requests.some(r=>r.sessionId!==identity.sessionId)||[...contributors.keys()].some(id=>!analysisIds.has(id)))refuse("analysis_projection_invalid");
 return {snapshotAt:value.snapshot_at,snapshotId:value.snapshot_id,payload:{outcome:"ok",sessionId:identity.sessionId,requests,analyses}};
})}

/** Separate public-CA TLS source pool; never reuse Supabase's pinned-root pool.
 * Drop URL TLS/options overrides so credentials cannot weaken this boundary. */
export function readonlyAnalysisPool(url:string,onError:(code:string)=>void=()=>{}):Pool {
 const parsed=new URL(url);if(!/^postgres(?:ql)?:$/u.test(parsed.protocol))throw new Error("analysis source: invalid postgres URL");
 for(const key of ["sslmode","sslrootcert","sslcert","sslkey","ssl","options"])parsed.searchParams.delete(key);
 const pool=new pg.Pool({connectionString:parsed.toString(),max:1,application_name:"96bulls-analysis-cache-readonly",ssl:{rejectUnauthorized:true},options:"-c default_transaction_read_only=on -c statement_timeout=15000",idleTimeoutMillis:60000});pool.on("error",error=>onError(/^[A-Z0-9_]{1,40}$/u.test(String((error as {code?:unknown}).code??""))?String((error as {code?:unknown}).code):"source_pool_error"));return pool;
}
