/** Pure v2 summary crypto contract, shared by producer and public reader.
 * No SQL, source capture, model execution, credential discovery or key loading.
 * Inputs must come from an authenticated current claim, never a browser key. */
import{createHash}from"node:crypto";
import{publicationDerivedKey}from"../../packages/object-store/src/index.ts";
import type{HistoryPublicSummaryClaim,HistorySummaryPointer}from"./history-public-contract.ts";
import{boundedObjectStore}from"./bounded-object-store.ts";
import{readBoundSummaryValue,type WorkspaceKeyReader,type SummaryReadStore}from"../bob-costas/summary-objects.ts";
import{canonicalTimestamp}from"../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
export type HistorySummaryContentSource={identity:HistoryPublicSummaryClaim["identity"];source:{epoch:string;fingerprint:string}}&
 Pick<NonNullable<HistoryPublicSummaryClaim["authority"]>,"authorityId"|"maskSha256"|"derivedAnchorKey">;
export function historySummaryContentBinding(c:HistorySummaryContentSource,kind:"short"|"long"):string{
 return createHash("sha256").update(JSON.stringify(["bob-summary-content-v2",
 {orgId:c.identity.orgId,workspaceId:c.identity.workspaceId,source:c.identity.source,sessionId:c.identity.sessionId},
 {epoch:c.source.epoch,fingerprint:c.source.fingerprint},c.authorityId,c.maskSha256,kind])).digest("hex");
}

/** Consumer can authenticate a current pointer without private ModelWork rows,
 * secret metadata or historical schema-generation guesses. Object key + current
 * logical content tuple are stable AAD; job/protocol provenance stays encrypted. */
export async function readHistorySummaryContent(args:{
 store:SummaryReadStore;keyring:WorkspaceKeyReader;captured:HistorySummaryContentSource;pointer:HistorySummaryPointer;signal:AbortSignal;
}):Promise<Record<string,unknown>|null>{
 const {pointer:p,captured:c}=args;
 if(p.status!=="ready")return null;
 if(!p.objectKey||!p.objectSha256||!p.keyId||typeof p.model!=="string"||!p.model)return null;
 const file=p.objectKey.slice(p.objectKey.lastIndexOf("/")+1);
 try{if(!file.endsWith(".bin")||String(publicationDerivedKey(c.derivedAnchorKey,file.slice(0,-4),"summary-"+p.kind))!==p.objectKey)return null}catch{return null}
 const v=await readBoundSummaryValue({store:boundedObjectStore(args.store,args.signal),keyring:args.keyring,ids:{orgId:c.identity.orgId,workspaceId:c.identity.workspaceId},
  key:p.objectKey,sha256:p.objectSha256,bytes:p.objectBytes??undefined,keyId:p.keyId,binding:historySummaryContentBinding(c,p.kind)});
 args.signal.throwIfAborted();
 const s=v?.source as any;
 if(!v||v.v!==2||v.status!=="ready"||v.kind!==p.kind||v.model!==p.model||typeof v.summary!=="string"||!v.summary.trim()||Buffer.byteLength(v.summary)>(p.kind==="short"?4096:24576)||canonicalTimestamp(String(v.built_at))!==canonicalTimestamp(p.builtAt)||!s||["orgId","workspaceId","source","sessionId"].some(k=>s.identity?.[k]!==c.identity[k as keyof typeof c.identity])||s.epoch!==c.source.epoch||s.fingerprint!==c.source.fingerprint||s.authorityId!==c.authorityId||s.maskSha256!==c.maskSha256)return null;
 return v;
}
