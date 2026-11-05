import { layoutComponent } from "../shared/layout-component.ts";
/** Portable trusted Node worker; dependencies supply approved runtime identities.
 * No credential values or automatic production startup are defined here. */
import { randomUUID } from "node:crypto";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import { nodeReadBudget } from "../session-read-api/node-read-profile.ts";
import { indexSearchScope, type SearchIndexDeps } from "./search-index.ts";
import { produceCanonicalReplay, type ReplayProducerDeps } from "./replay-publication.ts";
import type { HistorySourceCapture } from "../shared/history-source-authority.ts";
import type { SessionScope } from "./pointer.ts";
export interface LargeReadJob { identity:{orgId:string;workspaceId:string;source:string;sessionId:string};source:{epoch:string;fingerprint:string};leaseNonce:string;schemaGeneration?:string }
export interface LargeReadDeps { expectedGeneration?:string; history?:(job:LargeReadJob)=>Promise<HistorySourceCapture>; db:SqlExecutor;search:SearchIndexDeps;replay:ReplayProducerDeps;signal?:AbortSignal;heartbeatMs?:number }
function scopeOf(j:LargeReadJob):SessionScope{return {orgId:j.identity.orgId,workspaceId:j.identity.workspaceId,harness:j.identity.source,sessionId:j.identity.sessionId}}
const args=(s:SessionScope)=>[s.orgId,s.workspaceId,s.harness,s.sessionId];
export async function routeLargeRead(db:SqlExecutor,s:SessionScope):Promise<boolean>{
 const r=(await db.query<{r:{delegated:boolean}}>("SELECT sessions.route_session_large_read($1,$2,$3,$4) r",args(s))).rows[0]?.r;
 if(!r||typeof r.delegated!=="boolean")throw new Error("large_handoff_invalid");return r.delegated;
}
export async function runLargeReadOnce(deps:LargeReadDeps):Promise<"idle"|"published"|"retry"|"lease_lost">{
 if(deps.signal?.aborted)return "idle";
 const interval=deps.heartbeatMs??30000;if(interval<100||interval>30000)throw new Error("large_heartbeat_invalid");
 const generation=deps.expectedGeneration;
 if(generation!==undefined&&(!/^[a-f0-9]{64}$/.test(generation)||!deps.history))throw new Error("large_history_configuration_required");
 const nonce=randomUUID();
 const managedJob=async(action:"claim"|"heartbeat"|"retry",identity?:LargeReadJob["identity"])=>{
  const payload={generation,action,leaseNonce:nonce,...(identity?{identity}:{})};
  return (await deps.db.query<{r:LargeReadJob|{ok:boolean}|null}>("SELECT sessions.session_history_large_job($1::jsonb) r",[JSON.stringify(payload)])).rows[0]?.r;
 };
 const j=(generation?await managedJob("claim"):(await deps.db.query<{job:LargeReadJob|null}>("SELECT sessions.claim_session_large_read($1::uuid,120) job",[nonce])).rows[0]?.job) as LargeReadJob|null|undefined;
 if(j===null)return "idle";
 if(!j||j.leaseNonce!==nonce||(generation&&j.schemaGeneration!==generation)||!j.identity||!/^[1-9][0-9]*$/.test(j.source?.epoch??"")||!/^[0-9a-f]{64}$/.test(j.source?.fingerprint??""))throw new Error("large_claim_invalid");
 const scope=scopeOf(j),params=[...args(scope),nonce];let lost=false,beating=false;
 const heartbeat=async()=>{if(beating||lost)return;beating=true;try{if(generation){const r=await managedJob("heartbeat",j.identity);if(!r||!("ok"in r)||r.ok!==true)lost=true;return;}const r=await deps.db.query<{ok:boolean}>("SELECT sessions.heartbeat_session_large_read($1,$2,$3,$4,$5::uuid) ok",params);if(r.rows[0]?.ok!==true)lost=true}catch{lost=true}finally{beating=false}};
 const timer=setInterval(()=>void heartbeat(),interval);
 const active=()=>{if(lost||deps.signal?.aborted)throw new Error("large_lease_lost")};
 try{
  active();await heartbeat();active();
  const captured=generation
   ?(await deps.db.query<{c:any}>("SELECT sessions.session_history_capture($1::jsonb) c",[JSON.stringify({generation,identity:j.identity})])).rows[0]?.c
   :(await deps.db.query<{c:any}>("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) c",args(scope))).rows[0]?.c;
  if(!captured?.exists||captured.blocked||captured.source?.epoch!==j.source.epoch||captured.source?.fingerprint!==j.source.fingerprint)throw new Error("large_source_changed");
  const physical=layoutComponent(j.identity.sessionId);
  if(!captured.snapshot?.pointers?.some((p:any)=>p.source_session_id===physical))throw new Error("large_physical_source_missing");
  const history=deps.history?await deps.history(j):deps.replay.history;
  if(generation&&history?.schemaGeneration!==generation)throw new Error("large_history_generation_mismatch");
  if(captured.snapshot?.history?.required===true&&!history)throw new Error("large_history_authority_required");
  const indexScope={...scope,sessionId:physical},budget=nodeReadBudget("large");
  // Managed images use610's source-bound feature publisher. The old physical
  // transcript_events indexer cannot attest a logical History winner mask.
  if(!history){
   const indexed=await indexSearchScope(indexScope,{...deps.search,limits:{...deps.search.limits,maxScopeBytes:budget.sourceBytes,maxScopeEvents:budget.sourceRecords,maxRows:budget.projectedRows}});
   if(indexed.busy||!indexed.complete)throw new Error("large_index_incomplete");
  }
  active();
  const result=await produceCanonicalReplay(scope,{...deps.replay,history,largeLease:nonce,profile:"large",publish:async publishParams=>{
   active();if(String(publishParams[4])!==j.source.epoch||publishParams[5]!==j.source.fingerprint)return false;
   const r=await deps.db.query<{ok:boolean}>("SELECT sessions.publish_session_large_read($1,$2,$3,$4,$5::bigint,$6,$7::jsonb,$8,$9,$10,$11,$12,$13::integer,$14::uuid) ok",[...publishParams,nonce]);
   return r.rows[0]?.ok===true;
  }});
  if(!result.published)throw new Error("large_publication_unavailable");
  return "published";
 }catch{
  if(lost||deps.signal?.aborted)return "lease_lost";
  if(generation){const r=await managedJob("retry",j.identity);return r&&"ok"in r&&r.ok===true?"retry":"lease_lost";}
  const r=await deps.db.query<{ok:boolean}>("SELECT sessions.retry_session_large_read($1,$2,$3,$4,$5::uuid,$6) ok",[...params,"large_processing_failed"]);
  return r.rows[0]?.ok===true?"retry":"lease_lost";
 }finally{clearInterval(timer)}
}
