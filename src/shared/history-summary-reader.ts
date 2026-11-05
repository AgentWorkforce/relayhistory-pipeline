/** Pure authenticated-claim reader. The outer transport owns bearer/auth/scope.
 * This module never chooses a credential, loads an env key, calls an unscoped
 * RPC, accepts an arbitrary key, or interprets missing data as verified empty. */
import type{ObjectStore}from"../../packages/object-store/src/index.ts";
import{HISTORY_PUBLIC_SUMMARY_VERSION,type HistoryPublicSummaryClaim,type HistoryPublicSummaryKind}from"./history-public-contract.ts";
import type{HistorySummaryContentSource}from"./history-summary-content.ts";
import{canonicalTimestamp}from"../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import{readHistorySummaryContent}from"./history-summary-content.ts";
import{MAX_SUMMARY_OBJECT_BYTES,type WorkspaceIds}from"../bob-costas/summary-objects.ts";
import{withAbortSignal}from"./bounded-object-store.ts";

export const HISTORY_SUMMARY_CLAIM_MAX_BYTES=256*1024;
type Identity=HistoryPublicSummaryClaim["identity"];
type Storage=NonNullable<HistoryPublicSummaryClaim["storage"]>;
type State=HistoryPublicSummaryClaim["state"];
type Source={epoch:string;fingerprint:string};
export interface HistorySummaryReadKeyring{
 /** Remote implementations MUST cancel their lookup when signal aborts.
  * Local two-argument WorkspaceKeyReader implementations remain assignable. */
 keyById(ids:WorkspaceIds,keyId:string,signal:AbortSignal):Promise<CryptoKey>;
}
export interface HistorySummaryReadDeps{
 /** Fixed original bearer and original RPC arguments. Never cache final call. */
 claim(signal:AbortSignal):Promise<unknown>;
 /** One configured deployment target, not a caller-selected bucket/region. */
 storage:Storage;
 read(key:string,signal:AbortSignal):Promise<Awaited<ReturnType<ObjectStore["getObject"]>>>;
 keyring?:HistorySummaryReadKeyring;
}
export type HistorySummaryReadResult=
 |{status:"unavailable"}
 |{status:"empty";kind:HistoryPublicSummaryKind;source:Source;state:State}
 |{status:"ready";kind:HistoryPublicSummaryKind;summary:string;model:string;builtAt:string;source:Source;state:State};
const HEX=/^[a-f0-9]{64}$/,UUID=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/,DEC=/^(0|[1-9][0-9]{0,18})$/;
const unavailable=():HistorySummaryReadResult=>({status:"unavailable"});
function object(v:unknown):v is Record<string,any>{return!!v&&typeof v==="object"&&!Array.isArray(v)}
function exact(v:unknown,keys:readonly string[]):v is Record<string,any>{return object(v)&&Object.keys(v).sort().join(",")===[...keys].sort().join(",")}
const identityKeys=["orgId","workspaceId","source","sessionId"]as const;
function sameIdentity(a:Identity,b:Identity){return identityKeys.every(k=>a[k]===b[k])}
function decimal(v:unknown,positive=false):v is string{return typeof v==="string"&&DEC.test(v)&&BigInt(v)<=9223372036854775807n&&(!positive||BigInt(v)>0n)}
function time(v:unknown):string|null{if(typeof v!=="string"||!v)return null;try{return canonicalTimestamp(v)}catch{return null}}
/** Detached validated snapshot: a transport cannot mutate its first claim while
 * encrypted IO is running. Bounds apply before copying or accepting fields. */
export function checkedHistorySummaryClaim(value:unknown,expected:{identity:Identity;kind:HistoryPublicSummaryKind}):HistoryPublicSummaryClaim|null{
 let encoded:string|undefined;try{encoded=JSON.stringify(value)}catch{return null}
 if(typeof encoded!=="string"||Buffer.byteLength(encoded)>HISTORY_SUMMARY_CLAIM_MAX_BYTES)return null;
 const c=JSON.parse(encoded);
 if(!exact(c,["version","kind","exists","required","ready","identity","source","authority","storage","pointer","state"])||
  c.version!==HISTORY_PUBLIC_SUMMARY_VERSION||c.kind!==expected.kind||typeof c.exists!=="boolean"||typeof c.required!=="boolean"||typeof c.ready!=="boolean"||
  !exact(c.identity,identityKeys)||identityKeys.some(k=>typeof c.identity[k]!=="string"||(k!=="workspaceId"&&!c.identity[k]))||!sameIdentity(c.identity as Identity,expected.identity)||
  !exact(c.source,["epoch","fingerprint"])||typeof c.source.fingerprint!=="string"||(c.source.epoch!==null&&!decimal(c.source.epoch,true)))return null;
 if(!c.ready){return c.authority===null&&c.storage===null&&c.pointer===null&&c.state===null?c as HistoryPublicSummaryClaim:null}
 if(!c.exists||!c.required||!decimal(c.source.epoch,true)||!HEX.test(c.source.fingerprint)||
  !exact(c.authority,["authorityId","maskSha256","recordCount","derivedAnchorKey","schemaGeneration"])||
  typeof c.authority.authorityId!=="string"||!UUID.test(c.authority.authorityId)||typeof c.authority.maskSha256!=="string"||!HEX.test(c.authority.maskSha256)||!decimal(c.authority.recordCount)||typeof c.authority.derivedAnchorKey!=="string"||!c.authority.derivedAnchorKey||typeof c.authority.schemaGeneration!=="string"||!HEX.test(c.authority.schemaGeneration)||
  !exact(c.storage,["provider","bucket","region"])||c.storage.provider!=="s3"||typeof c.storage.bucket!=="string"||!c.storage.bucket||typeof c.storage.region!=="string"||!c.storage.region)return null;
 if(c.pointer!==null){
  const p=c.pointer;
  if(!exact(p,["kind","status","objectKey","objectSha256","objectBytes","keyId","model","builtAt"])||p.kind!==c.kind||!["ready","empty","disabled","quarantined","unsupported"].includes(p.status))return null;
  const at=time(p.builtAt);if(!at)return null;p.builtAt=at;
  if(p.status==="ready"&&(typeof p.objectKey!=="string"||!p.objectKey||typeof p.objectSha256!=="string"||!HEX.test(p.objectSha256)||!Number.isSafeInteger(p.objectBytes)||p.objectBytes<1||p.objectBytes>MAX_SUMMARY_OBJECT_BYTES||typeof p.keyId!=="string"||!p.keyId||typeof p.model!=="string"||!p.model))return null;
  if(p.status==="empty"&&["objectKey","objectSha256","objectBytes","keyId","model"].some(k=>p[k]!==null))return null;
 }
 if(c.state!==null){if(!exact(c.state,["state","classifiedAt"])||!["active","done","abandoned","unknown"].includes(c.state.state))return null;const at=time(c.state.classifiedAt);if(!at)return null;c.state.classifiedAt=at}
 return c as HistoryPublicSummaryClaim;
}
/** Explicit canonical constructor: equality does not depend on JSONB key order. */
function fingerprint(c:HistoryPublicSummaryClaim):string{
 const p=c.pointer,a=c.authority,s=c.storage;
 return JSON.stringify([c.version,c.kind,c.exists,c.required,c.ready,identityKeys.map(k=>c.identity[k]),
 [c.source.epoch,c.source.fingerprint],a&&[a.authorityId,a.maskSha256,a.recordCount,a.derivedAnchorKey,a.schemaGeneration],
 s&&[s.provider,s.bucket,s.region],p&&[p.kind,p.status,p.objectKey,p.objectSha256,p.objectBytes,p.keyId,p.model,p.builtAt],
 c.state&&[c.state.state,c.state.classifiedAt]]);
}
export async function readClaimedHistorySummary(deps:HistorySummaryReadDeps,args:{identity:Identity;kind:HistoryPublicSummaryKind;signal:AbortSignal}):Promise<HistorySummaryReadResult>{
 const {signal}=args;signal.throwIfAborted();if(!["short","long"].includes(args.kind))return unavailable();
 const first=checkedHistorySummaryClaim(await withAbortSignal(()=>deps.claim(signal),signal),args);signal.throwIfAborted();
 if(!first?.exists||!first.required||!first.ready||!first.authority||!first.storage||first.pointer===null||
  ["provider","bucket","region"].some(k=>first.storage![k as keyof Storage]!==deps.storage[k as keyof Storage]))return unavailable();
 const source={epoch:first.source.epoch!,fingerprint:first.source.fingerprint},p=first.pointer;
 if(p.status!=="ready"&&p.status!=="empty")return unavailable();
 let summary:Record<string,unknown>|null=null;
 if(p.status==="ready"){
  if(typeof deps.keyring?.keyById!=="function")return unavailable();
  const captured:HistorySummaryContentSource={identity:first.identity,source,...first.authority};
  const port=deps.keyring;
  try{
   summary=await readHistorySummaryContent({captured,pointer:p,signal,
    store:{getObject:async key=>{signal.throwIfAborted();if(String(key)!==p.objectKey)throw Error("history_summary_object_scope");return deps.read(String(key),signal)}},
    keyring:{keyById:async(ids,id)=>{
     signal.throwIfAborted();
     if(ids.orgId!==first.identity.orgId||ids.workspaceId!==first.identity.workspaceId||id!==p.keyId)throw Error("history_summary_key_scope");
     const key=await withAbortSignal(()=>port.keyById(ids,id,signal),signal);signal.throwIfAborted();return key;
    }}});
  }catch{signal.throwIfAborted();return unavailable()}
  signal.throwIfAborted();if(!summary)return unavailable();
 }
 const final=checkedHistorySummaryClaim(await withAbortSignal(()=>deps.claim(signal),signal),args);signal.throwIfAborted();
 if(!final||fingerprint(final)!==fingerprint(first))return unavailable();
 return p.status==="empty"?{status:"empty",kind:args.kind,source,state:first.state}:
  {status:"ready",kind:args.kind,source,state:first.state,summary:summary!.summary as string,model:p.model!,builtAt:p.builtAt};
}
