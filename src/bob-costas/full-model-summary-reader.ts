/** Backend bridge core. Caller MUST authenticate/authorize the explicit scope
 * before supplying its dedicated metadata/key ports. No JWT inference here.
 * Same original source/pointer recheck after object+key IO; no SQL lock across IO.
 * This is not public RHC/Cloud route wiring or a credential provisioning path. */
import type{SqlExecutor}from"../../packages/pipeline/src/lib/pipeline/queue.ts";
import type{ObjectStore}from"../../packages/object-store/src/index.ts";
import type{HistoryScope}from"../shared/history-visibility.ts";
import{captureHistorySourceReference,readHistorySummary,type HistorySummaryPointer}from"../shared/history-source-authority.ts";
import type{WorkspaceKeyring}from"./summary-objects.ts";
import{readFullModelSummary}from"./full-model-artifacts.ts";
function same(a:HistorySummaryPointer|null,b:HistorySummaryPointer):boolean{
 return a!==null&&(["kind","status","objectKey","objectSha256","objectBytes","keyId","model","builtAt"]as const).every(k=>a[k]===b[k]);
}
export async function readCurrentFullModelSummary(args:{
 db:SqlExecutor;store:ObjectStore;keyring:WorkspaceKeyring;scope:HistoryScope;schemaGeneration:string;
 kind:"short"|"long";signal:AbortSignal;
}):Promise<{status:"unavailable"}|{status:"ready";summary:string;model:string;builtAt:string;source:{epoch:string;fingerprint:string}}>{
 args.signal.throwIfAborted();
 const captured=await captureHistorySourceReference(args.db,args.scope,{schemaGeneration:args.schemaGeneration,profile:"large",signal:args.signal});
 const pointer=await readHistorySummary(args.db,captured,args.kind);
 if(!pointer||pointer.status!=="ready")return{status:"unavailable"};
 const value=await readFullModelSummary({...args,captured,pointer});
 if(!value)return{status:"unavailable"};
 args.signal.throwIfAborted();
 const fresh=await readHistorySummary(args.db,captured,args.kind);
 args.signal.throwIfAborted();
 if(!same(fresh,pointer))return{status:"unavailable"};
 return{status:"ready",summary:value.summary as string,model:value.model as string,builtAt:pointer.builtAt,source:captured.source};
}
