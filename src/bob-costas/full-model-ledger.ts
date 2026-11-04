/** Typed metadata-only630 RPC client. Never owns a DB role/table or rebases an
 * old lease onto a newly captured source. Call through the bounded PG facade. */
import type{SqlExecutor}from"../../packages/pipeline/src/lib/pipeline/queue.ts";
import type{ArtifactPublication}from"../session-read-api/canonical-artifact.ts";
import type{HistorySourceReference,HistorySummaryPointer}from"../shared/history-source-authority.ts";
import{fullModelGraphBinding}from"./full-model-input.ts";
import{fullModelConfigHash,initialFullModelCheckpoint,validateFullModelCheckpoint,type FullModelCheckpoint}from"./full-model-work.ts";
export interface FullModelWorkConfig{protocolSha256:string;model:string;keyId:string;graph:ArtifactPublication}
export interface FullModelTicket{
 version:1;workId:string;leaseToken:string;leaseUntil:string;checkpointVersion:string;
 captured:HistorySourceReference;config:FullModelWorkConfig;checkpoint:FullModelCheckpoint;
}
const UUID=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/,HEX=/^[a-f0-9]{64}$/,DEC=/^(0|[1-9][0-9]{0,18})$/;
function no(code:string):never{throw Object.assign(Error(code),{code})}
function evidence(c:HistorySourceReference){return{generation:c.schemaGeneration,identity:c.identity,source:c.source,authorityId:c.authorityId,maskSha256:c.maskSha256}}
async function rpc(db:SqlExecutor,fn:string,p:unknown){
 const r=await db.query<{result:any}>("SELECT sessions.session_history_model_"+fn+"($1::jsonb) AS result",[JSON.stringify(p)]);
 if(r.rows.length!==1||!r.rows[0]?.result||typeof r.rows[0].result!=="object")no("model_work_rpc");
 return r.rows[0].result;
}
export function modelWorkConfig(model:string,keyId:string,graph:ArtifactPublication):FullModelWorkConfig{return{model,keyId,graph,protocolSha256:fullModelConfigHash(model,keyId)}}
export async function enqueueFullModelWork(db:SqlExecutor,captured:HistorySourceReference,config:FullModelWorkConfig,wakeToken?:string):Promise<{current:false}|{current:true;workId:string;complete:boolean}>{
 if(config.protocolSha256!==fullModelConfigHash(config.model,config.keyId))no("model_work_config");
 const checkpoint=initialFullModelCheckpoint(fullModelGraphBinding(captured,config.graph),config.protocolSha256);
 const r=await rpc(db,"enqueue",{evidence:evidence(captured),captured,config,checkpoint,...(wakeToken?{wakeToken}:{})});
 if(r.current===false)return{current:false};
 if(r.current!==true||!UUID.test(r.workId)||!["pending","complete","failed","canceled"].includes(r.status))no("model_work_enqueue");
 return{current:true,workId:r.workId,complete:r.status==="complete"};
}
export function checkedFullModelTicket(v:unknown,generation:string):FullModelTicket{
 const t=v as FullModelTicket,c=t?.captured;
 if(!t||t.version!==1||!UUID.test(t.workId)||!UUID.test(t.leaseToken)||!Number.isFinite(Date.parse(t.leaseUntil))||!DEC.test(t.checkpointVersion)||!c||c.schemaGeneration!==generation||!HEX.test(generation)||!UUID.test(c.authorityId)||!HEX.test(c.maskSha256)||!HEX.test(c.source?.fingerprint)||!DEC.test(c.source?.epoch)||BigInt(c.source.epoch)<1n||!DEC.test(c.recordCount)||typeof c.derivedAnchorKey!=="string"||!c.identity||["orgId","workspaceId","source","sessionId"].some(k=>typeof c.identity[k as keyof typeof c.identity]!=="string"||!c.identity[k as keyof typeof c.identity])||!t.config||t.config.protocolSha256!==fullModelConfigHash(t.config.model,t.config.keyId))no("model_work_ticket");
 validateFullModelCheckpoint(t.checkpoint,fullModelGraphBinding(c,t.config.graph),t.config.protocolSha256);
 return t;
}
export async function claimFullModelWork(db:SqlExecutor,generation:string,model:string):Promise<FullModelTicket|null>{
 if(!HEX.test(generation))no("model_work_generation");
 const r=await rpc(db,"claim",{generation,leaseSeconds:120,model});
 if(r.ticket===null)return null;return checkedFullModelTicket(r.ticket,generation);
}
const lease=(t:FullModelTicket)=>({workId:t.workId,leaseToken:t.leaseToken,checkpointVersion:t.checkpointVersion,generation:t.captured.schemaGeneration});
export async function heartbeatFullModelWork(db:SqlExecutor,t:FullModelTicket):Promise<boolean>{
 const r=await rpc(db,"heartbeat",lease(t));
 if(typeof r.renewed!=="boolean")no("model_work_heartbeat");
 return r.renewed;
}
export async function checkpointFullModelWork(db:SqlExecutor,t:FullModelTicket,next:FullModelCheckpoint,done?:{
 pointers:HistorySummaryPointer[];state:"active"|"done"|"abandoned"|"unknown"|null;classifiedAt:string;
}):Promise<boolean>{
 validateFullModelCheckpoint(next,fullModelGraphBinding(t.captured,t.config.graph),t.config.protocolSha256);
 const r=await rpc(db,"checkpoint",{...lease(t),next,done:!!done,...done});
 if(typeof r.advanced!=="boolean")no("model_work_checkpoint");
 if(r.advanced&&(r.checkpointVersion!==(BigInt(t.checkpointVersion)+1n).toString()||r.complete!==!!done))no("model_work_checkpoint");
 return r.advanced;
}

export async function cancelFullModelWork(db:SqlExecutor,t:FullModelTicket):Promise<boolean>{
 const r=await rpc(db,"checkpoint",{...lease(t),next:t.checkpoint,done:false,cancel:true,cancelReason:"config_changed"});
 if(typeof r.advanced!=="boolean")no("model_work_checkpoint");return r.advanced;
}

export interface FullModelWake{wakeToken:string;identity:HistorySourceReference["identity"];source:HistorySourceReference["source"];authorityId:string;maskSha256:string}
export async function discoverFullModelScopes(db:SqlExecutor,generation:string):Promise<FullModelWake[]>{
 if(!HEX.test(generation))no("model_work_generation");
 const r=await rpc(db,"discover",{generation});
 if(!Array.isArray(r.scopes)||r.scopes.length>20)no("model_work_discovery");
 const tokens=new Set<string>();
 for(const s of r.scopes){
  if(!UUID.test(s?.wakeToken)||tokens.has(s.wakeToken)||!UUID.test(s.authorityId)||!HEX.test(s.maskSha256)||!HEX.test(s.source?.fingerprint)||!DEC.test(s.source?.epoch)||BigInt(s.source.epoch)<1n||!s.identity||["orgId","workspaceId","source","sessionId"].some(k=>typeof s.identity[k]!=="string"||!s.identity[k]))no("model_work_discovery");
  tokens.add(s.wakeToken);
 }
 return r.scopes;
}

export async function failFullModelWork(db:SqlExecutor,t:FullModelTicket,error:{code:string;retryable:boolean}):Promise<boolean>{
 if(!/^[a-z0-9_]{1,64}$/.test(error.code))no("model_failure_code");
 const r=await rpc(db,"checkpoint",{...lease(t),next:t.checkpoint,done:false,error});
 if(typeof r.advanced!=="boolean")no("model_work_checkpoint");return r.advanced;
}
