/** Logical source authority for replay, CH and model consumers. No payload/model
 * IO may run inside withHistorySourceAuthority's final metadata callback. */
import type{SqlExecutor}from"../../packages/pipeline/src/lib/pipeline/queue.ts";
import type{Transact}from"./db.ts";
import{publicationDerivedKey,parseSessionKey,parseUploadGenerationPrefix}from"../../packages/object-store/src/index.ts";
import{nodeReadBudget,type NodeReadProfile}from"../session-read-api/node-read-profile.ts";
import type{HistoryScope,HistoryLogicalMask,HistoryCurrentSeat}from"./history-visibility.ts";
export interface HistorySourceCapture{
 readonly identity:Readonly<HistoryScope>;readonly source:Readonly<{epoch:string;fingerprint:string}>;
 readonly authorityId:string;readonly maskSha256:string;readonly recordCount:string;
 readonly derivedAnchorKey:string;readonly schemaGeneration:string;readonly mask:HistoryLogicalMask;
}
export interface HistorySourceOptions{schemaGeneration:string;profile?:NodeReadProfile;signal?:AbortSignal}
export interface HistorySummaryPointer{
 kind:"short"|"long";status:"ready"|"disabled"|"quarantined"|"empty"|"unsupported";
 objectKey:string|null;objectSha256:string|null;objectBytes:number|null;keyId:string|null;model:string|null;builtAt:string;
}
const HEX=/^[a-f0-9]{64}$/,UUID=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/,DEC=/^(0|[1-9][0-9]{0,18})$/,MAX=9223372036854775807n;
function no(code:string):never{throw Object.assign(Error(code),{code})}
function positive(v:unknown):v is string{return typeof v==="string"&&DEC.test(v)&&BigInt(v)>0n&&BigInt(v)<=MAX}
function descriptor(c:any,scope:HistoryScope){
 const h=c?.snapshot?.history;
 if(!c||c.exists!==true||c.blocked!==false||c.identity?.orgId!==scope.orgId||c.identity.workspaceId!==scope.workspaceId||c.identity.source!==scope.source||c.identity.sessionId!==scope.sessionId||!positive(c.source?.epoch)||!HEX.test(c.source?.fingerprint)||c.snapshot.epoch!==c.source.epoch||
  !h||h.version!==1||h.required!==true||!UUID.test(h.authorityId)||!HEX.test(h.maskSha256)||typeof h.recordCount!=="string"||!DEC.test(h.recordCount)||!parseUploadGenerationPrefix(parseSessionKey(h.derivedAnchorKey??"")?.prefix))no("history_source_unavailable");
 return h as{version:1;required:true;authorityId:string;maskSha256:string;recordCount:string;derivedAnchorKey:string};
}
const evidence=(c:HistorySourceCapture)=>({generation:c.schemaGeneration,identity:c.identity,source:c.source,authorityId:c.authorityId,maskSha256:c.maskSha256});
async function result(db:SqlExecutor,fn:string,p:unknown){const r=await db.query<{result:any}>("SELECT sessions.session_history_"+fn+"($1::jsonb) AS result",[JSON.stringify(p)]);if(r.rows.length!==1||!r.rows[0]?.result||typeof r.rows[0].result!=="object")no("history_source_rpc");return r.rows[0].result;}
/** Requires an explicit complete620 descriptor. Missing mask is never empty. */
export async function captureHistorySource(db:SqlExecutor,scope:HistoryScope,options:HistorySourceOptions):Promise<HistorySourceCapture>{
 if(!HEX.test(options.schemaGeneration))no("history_source_generation");const budget=nodeReadBudget(options.profile),signal=options.signal;
 const capture=async()=>{signal?.throwIfAborted();const r=await db.query<{captured:any}>("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4)AS captured",[scope.orgId,scope.workspaceId,scope.source,scope.sessionId]);if(r.rows.length!==1)no("history_source_unavailable");signal?.throwIfAborted();return r.rows[0]?.captured};
 const first=await capture(),h=descriptor(first,scope);if(BigInt(h.recordCount)>BigInt(budget.sourceRecords))no("history_mask_record_bound");
 const winners=new Map<string,Readonly<HistoryCurrentSeat>|null>();let after="0",count=0n,bytes=0;
 const provisional={identity:{...scope},source:{epoch:first.source.epoch,fingerprint:first.source.fingerprint},authorityId:h.authorityId,maskSha256:h.maskSha256,recordCount:h.recordCount,derivedAnchorKey:h.derivedAnchorKey,schemaGeneration:options.schemaGeneration};
 while(true){
  signal?.throwIfAborted();const page=await result(db,"mask_page",{...evidence(provisional as HistorySourceCapture),after});signal?.throwIfAborted();
  if(page.authorityId!==h.authorityId||page.total!==h.recordCount||!Array.isArray(page.records)||page.records.length>500||!(page.next===null||typeof page.next==="string"&&DEC.test(page.next)))no("history_mask_page");
  const size=Buffer.byteLength(JSON.stringify(page));if(size>16*1024**2||bytes+size>budget.sourceBytes)no("history_mask_byte_bound");bytes+=size;
  for(const row of page.records){if(row.ordinal!==String(count++)||typeof row.recordId!=="string"||!row.recordId||winners.has(row.recordId)||!(row.winner===null||row.winner&&typeof row.winner==="object"))no("history_mask_coverage");
   if(row.winner!==null){const s=row.winner;if(s.recordId!==row.recordId||typeof s.originId!=="string"||!s.originId||!positive(s.revision)||!positive(s.acceptedSeq)||!HEX.test(s.contentSha256)||typeof s.lineage!=="string"||!DEC.test(s.lineage)||typeof s.userId!=="string"||!s.userId||typeof s.source!=="string"||!s.source||typeof s.kind!=="string"||!s.kind||!(s.sessionId===null||typeof s.sessionId==="string"&&s.sessionId)||!["upsert","delete"].includes(s.operation))no("history_mask_seat");winners.set(row.recordId,Object.freeze({...s}));}
   else winners.set(row.recordId,null);
  }
  if(page.next===null)break;if(!page.records.length||BigInt(page.next)<=BigInt(after)||BigInt(page.next)!==count)no("history_mask_cursor");after=page.next;
 }
 if(count!==BigInt(h.recordCount))no("history_mask_incomplete");
 const last=await capture(),lastH=descriptor(last,scope);if(last.source.epoch!==first.source.epoch||last.source.fingerprint!==first.source.fingerprint||JSON.stringify(lastH)!==JSON.stringify(h))no("history_source_changed");
 const identity=Object.freeze({...scope}),source=Object.freeze({...provisional.source});
 return Object.freeze({...provisional,identity,source,mask:Object.freeze({scope:identity,source,winners})});
}
export async function withHistorySourceAuthority<T>(transact:Transact,captured:HistorySourceCapture,work:(tx:SqlExecutor)=>Promise<T>):Promise<{current:false}|{current:true;value:T}>{
 return transact(async tx=>{const checked=await result(tx,"source_lock",evidence(captured));if(typeof checked.current!=="boolean")no("history_source_lock");if(!checked.current)return{current:false}as const;return{current:true,value:await work(tx)}as const});
}
export function historyDerivedKey(captured:HistorySourceCapture,family:string,id:string):string{return String(publicationDerivedKey(captured.derivedAnchorKey,id,family))}
/** Exact immutable object intent BEFORE PUT; intermediate model slices use the
 * same namespace/registration and retire with their captured logical source. */
export async function registerHistoryDerivedObject(db:SqlExecutor,captured:HistorySourceCapture,spec:{key:string;sha256:string;bytes:number}):Promise<void>{
 if((await result(db,"register_object",{...evidence(captured),...spec})).registered!==true)no("history_object_registration");
}
/** Metadata-only callbacks: no transcript/summary text, no invented machine or
 * raw revision. Must execute inside withHistorySourceAuthority's transaction. */
export async function publishHistorySummary(tx:SqlExecutor,captured:HistorySourceCapture,pointer:HistorySummaryPointer):Promise<boolean>{
 const r=await result(tx,"summary_publish",{...evidence(captured),pointer});if(typeof r.applied!=="boolean")no("history_summary_publish");return r.applied;
}
export async function publishHistoryState(tx:SqlExecutor,captured:HistorySourceCapture,state:"active"|"done"|"abandoned"|"unknown",classifiedAt:string):Promise<boolean>{
 const r=await result(tx,"state_publish",{...evidence(captured),state,classifiedAt});if(typeof r.applied!=="boolean")no("history_state_publish");return r.applied;
}
