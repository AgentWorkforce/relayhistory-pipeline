/** Model plaintext exists only in RAM and authenticated encrypted objects.
 * Every immutable object is registered against the ORIGINAL captured logical
 * authority before PUT; no network call runs inside final publication SQL. */
import { createHash, randomUUID } from "node:crypto";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import { historyDerivedKey, registerHistoryDerivedObject, type HistorySourceReference, type HistorySummaryPointer } from "../shared/history-source-authority.ts";
import { boundedObjectStore } from "../shared/bounded-object-store.ts";
import { prepareBoundSummaryValue, putPreparedSummaryObject, readBoundSummaryValue, type WorkspaceKeyring } from "./summary-objects.ts";
import type { FullModelConclusion, FullModelCheckpoint, ModelSlice, ModelSliceRef } from "./full-model-work.ts";
import{canonicalTimestamp}from"../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import { fullModelSourceBinding } from "./full-model-input.ts";

export interface ModelArtifactDeps {
 db: SqlExecutor; store: ObjectStore; keyring: WorkspaceKeyring;
 captured: HistorySourceReference; workId: string; configSha256: string; sourceBinding:string; keyId:string;
}
const binding=(d:ModelArtifactDeps,purpose:string)=>createHash("sha256").update(JSON.stringify(["bob-work-envelope-v1",fullModelSourceBinding(d.captured),d.sourceBinding,d.workId,d.configSha256,purpose])).digest("hex");
function summaryBinding(c:HistorySourceReference,kind:"short"|"long"):string{
 return createHash("sha256").update(JSON.stringify(["bob-summary-content-v2",
 {orgId:c.identity.orgId,workspaceId:c.identity.workspaceId,source:c.identity.source,sessionId:c.identity.sessionId},
 {epoch:c.source.epoch,fingerprint:c.source.fingerprint},c.authorityId,c.maskSha256,kind])).digest("hex");
}
export function modelArtifactPorts(d:ModelArtifactDeps){
 if(!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(d.workId)||!/^[a-f0-9]{64}$/.test(d.configSha256))throw Error("model_artifact_identity");
 const ids={orgId:d.captured.identity.orgId,workspaceId:d.captured.identity.workspaceId};
 return {
  async writeSlice(value:ModelSlice,range:{first:string;count:string;level:number},signal:AbortSignal):Promise<ModelSliceRef>{
   signal.throwIfAborted();
   const id=randomUUID(),key=historyDerivedKey(d.captured,"bob-slice",id);
   const prepared=await prepareBoundSummaryValue({keyring:d.keyring,ids,key,binding:binding(d,"evidence"),value:value as unknown as Record<string,unknown>});
   if(prepared.keyId!==d.keyId)throw Error("model_key_changed");
   signal.throwIfAborted();
   await registerHistoryDerivedObject(d.db,d.captured,{key:prepared.key,sha256:prepared.sha256,bytes:prepared.bytes});
   signal.throwIfAborted();
   await putPreparedSummaryObject({store:boundedObjectStore(d.store,signal),prepared});
   signal.throwIfAborted();
   return {id,sha256:prepared.sha256,bytes:prepared.bytes,...range};
  },
  async readSlice(ref:ModelSliceRef,signal:AbortSignal):Promise<ModelSlice>{
   // Reject a forged/cross-source frontier key before any object fetch.
   const key=historyDerivedKey(d.captured,"bob-slice",ref.id);
   const value=await readBoundSummaryValue({store:boundedObjectStore(d.store,signal),keyring:d.keyring,ids,key,sha256:ref.sha256,bytes:ref.bytes,keyId:d.keyId,binding:binding(d,"evidence")});
   signal.throwIfAborted();
   if(!value)throw Error("model_slice_integrity");
   return value as unknown as ModelSlice;
  }
 };
}
/** Prepare BOTH pointers. Caller must publish them + state + completed ModelWork
 * checkpoint atomically under the original lease/source authority, then do its
 * FINAL wall-clock expiry check. Empty input never fabricates summary text. */
export async function prepareFullModelResults(d:ModelArtifactDeps,args:{
 conclusion:FullModelConclusion|null;checkpoint:FullModelCheckpoint;model:string;builtAt:string;signal:AbortSignal;
}):Promise<{pointers:HistorySummaryPointer[];state:null;classifiedAt:string}>{
 if(!Number.isFinite(Date.parse(args.builtAt))||args.checkpoint.sourceBinding!==d.sourceBinding||args.checkpoint.configSha256!==d.configSha256||!args.checkpoint.inputComplete)throw Error("model_result_binding");
 const ids={orgId:d.captured.identity.orgId,workspaceId:d.captured.identity.workspaceId},pointers:HistorySummaryPointer[]=[];
 for(const kind of ["short","long"] as const){
  args.signal.throwIfAborted();
  if(!args.conclusion){pointers.push({kind,status:"empty",objectKey:null,objectSha256:null,objectBytes:null,keyId:null,model:null,builtAt:args.builtAt});continue;}
  const key=historyDerivedKey(d.captured,"summary-"+kind,randomUUID());
  const value={v:2,kind,status:"ready",summary:args.conclusion[kind],model:args.model,built_at:args.builtAt,
   source:{identity:d.captured.identity,...d.captured.source,authorityId:d.captured.authorityId,maskSha256:d.captured.maskSha256},
   workId:d.workId,protocolSha256:d.configSha256,usage:{inputTokens:args.checkpoint.tokensIn,outputTokens:args.checkpoint.tokensOut,calls:args.checkpoint.calls},
   inputUnits:args.checkpoint.units,...(kind==="long"?{state:args.conclusion.state,stateReason:args.conclusion.stateReason}:{})};
  const prepared=await prepareBoundSummaryValue({keyring:d.keyring,ids,key,binding:summaryBinding(d.captured,kind),value});
  args.signal.throwIfAborted();
  await registerHistoryDerivedObject(d.db,d.captured,{key:prepared.key,sha256:prepared.sha256,bytes:prepared.bytes});
  args.signal.throwIfAborted();
  await putPreparedSummaryObject({store:boundedObjectStore(d.store,args.signal),prepared});
  pointers.push({kind,status:"ready",objectKey:prepared.key,objectSha256:prepared.sha256,objectBytes:prepared.bytes,keyId:prepared.keyId,model:args.model,builtAt:args.builtAt});
 }
 args.signal.throwIfAborted();
 // Evidence-only model state is retained encrypted in the long object. No
 // logical closed/endedAt policy is attested yet, so this cannot change the
 // authoritative Referee state from a physical-carrier or model guess.
 return {pointers,state:null,classifiedAt:args.builtAt};
}

/** Consumer can authenticate a current pointer without private ModelWork rows,
 * secret metadata or historical schema-generation guesses. Object key + current
 * logical content tuple are stable AAD; job/protocol provenance stays encrypted. */
export async function readFullModelSummary(args:{
 store:ObjectStore;keyring:WorkspaceKeyring;captured:HistorySourceReference;pointer:HistorySummaryPointer;signal:AbortSignal;
}):Promise<Record<string,unknown>|null>{
 const {pointer:p,captured:c}=args;
 if(p.status!=="ready")return null;
 if(!p.objectKey||!p.objectSha256||!p.keyId||typeof p.model!=="string"||!p.model)return null;
 const file=p.objectKey.slice(p.objectKey.lastIndexOf("/")+1);
 try{if(!file.endsWith(".bin")||historyDerivedKey(c,"summary-"+p.kind,file.slice(0,-4))!==p.objectKey)return null}catch{return null}
 const v=await readBoundSummaryValue({store:boundedObjectStore(args.store,args.signal),keyring:args.keyring,ids:{orgId:c.identity.orgId,workspaceId:c.identity.workspaceId},
  key:p.objectKey,sha256:p.objectSha256,bytes:p.objectBytes??undefined,keyId:p.keyId,binding:summaryBinding(c,p.kind)});
 args.signal.throwIfAborted();
 const s=v?.source as any;
 if(!v||v.v!==2||v.status!=="ready"||v.kind!==p.kind||v.model!==p.model||typeof v.summary!=="string"||!v.summary.trim()||Buffer.byteLength(v.summary)>(p.kind==="short"?4096:24576)||canonicalTimestamp(String(v.built_at))!==canonicalTimestamp(p.builtAt)||!s||["orgId","workspaceId","source","sessionId"].some(k=>s.identity?.[k]!==c.identity[k as keyof typeof c.identity])||s.epoch!==c.source.epoch||s.fingerprint!==c.source.fingerprint||s.authorityId!==c.authorityId||s.maskSha256!==c.maskSha256)return null;
 return v;
}
