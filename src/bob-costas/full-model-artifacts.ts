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
import { fullModelSourceBinding } from "./full-model-input.ts";

export interface ModelArtifactDeps {
 db: SqlExecutor; store: ObjectStore; keyring: WorkspaceKeyring;
 captured: HistorySourceReference; workId: string; configSha256: string;
}
const binding=(d:ModelArtifactDeps,purpose:string)=>createHash("sha256").update(JSON.stringify(["bob-work-envelope-v1",fullModelSourceBinding(d.captured),d.workId,d.configSha256,purpose])).digest("hex");
export function modelArtifactPorts(d:ModelArtifactDeps){
 if(!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(d.workId)||!/^[a-f0-9]{64}$/.test(d.configSha256))throw Error("model_artifact_identity");
 const ids={orgId:d.captured.identity.orgId,workspaceId:d.captured.identity.workspaceId};
 return {
  async writeSlice(value:ModelSlice,range:{first:string;count:string;level:number},signal:AbortSignal):Promise<ModelSliceRef>{
   signal.throwIfAborted();
   const key=historyDerivedKey(d.captured,"bob-slice",randomUUID());
   const prepared=await prepareBoundSummaryValue({keyring:d.keyring,ids,key,binding:binding(d,"evidence"),value:value as unknown as Record<string,unknown>});
   signal.throwIfAborted();
   await registerHistoryDerivedObject(d.db,d.captured,{key:prepared.key,sha256:prepared.sha256,bytes:prepared.bytes});
   signal.throwIfAborted();
   await putPreparedSummaryObject({store:boundedObjectStore(d.store,signal),prepared});
   signal.throwIfAborted();
   return {key:prepared.key,sha256:prepared.sha256,bytes:prepared.bytes,keyId:prepared.keyId,...range};
  },
  async readSlice(ref:ModelSliceRef,signal:AbortSignal):Promise<ModelSlice>{
   // Reject a forged/cross-source frontier key before any object fetch.
   const file=ref.key.slice(ref.key.lastIndexOf("/")+1);
   if(!file.endsWith(".bin")||historyDerivedKey(d.captured,"bob-slice",file.slice(0,-4))!==ref.key)throw Error("model_slice_namespace");
   const value=await readBoundSummaryValue({store:boundedObjectStore(d.store,signal),keyring:d.keyring,ids,...ref,binding:binding(d,"evidence")});
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
}):Promise<{pointers:HistorySummaryPointer[];state:"active"|"done"|"abandoned"|"unknown";classifiedAt:string}>{
 if(!Number.isFinite(Date.parse(args.builtAt))||args.checkpoint.sourceBinding!==fullModelSourceBinding(d.captured)||args.checkpoint.configSha256!==d.configSha256||!args.checkpoint.inputComplete)throw Error("model_result_binding");
 const ids={orgId:d.captured.identity.orgId,workspaceId:d.captured.identity.workspaceId},pointers:HistorySummaryPointer[]=[];
 for(const kind of ["short","long"] as const){
  args.signal.throwIfAborted();
  if(!args.conclusion){pointers.push({kind,status:"empty",objectKey:null,objectSha256:null,objectBytes:null,keyId:null,model:null,builtAt:args.builtAt});continue;}
  const key=historyDerivedKey(d.captured,"summary-"+kind,randomUUID());
  const value={v:2,kind,status:"ready",summary:args.conclusion[kind],model:args.model,built_at:args.builtAt,
   source:{identity:d.captured.identity,...d.captured.source,authorityId:d.captured.authorityId,maskSha256:d.captured.maskSha256},
   workId:d.workId,protocolSha256:d.configSha256,usage:{inputTokens:args.checkpoint.tokensIn,outputTokens:args.checkpoint.tokensOut,calls:args.checkpoint.calls},
   inputUnits:args.checkpoint.units,...(kind==="long"?{state:args.conclusion.state,stateReason:args.conclusion.stateReason}:{})};
  const prepared=await prepareBoundSummaryValue({keyring:d.keyring,ids,key,binding:binding(d,"summary-"+kind),value});
  args.signal.throwIfAborted();
  await registerHistoryDerivedObject(d.db,d.captured,{key:prepared.key,sha256:prepared.sha256,bytes:prepared.bytes});
  args.signal.throwIfAborted();
  await putPreparedSummaryObject({store:boundedObjectStore(d.store,args.signal),prepared});
  pointers.push({kind,status:"ready",objectKey:prepared.key,objectSha256:prepared.sha256,objectBytes:prepared.bytes,keyId:prepared.keyId,model:args.model,builtAt:args.builtAt});
 }
 args.signal.throwIfAborted();
 return {pointers,state:args.conclusion?.state??"unknown",classifiedAt:args.builtAt};
}
