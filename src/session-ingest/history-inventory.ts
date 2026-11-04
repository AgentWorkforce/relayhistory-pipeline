/** Full-source frontier only. The initial buffer receiver never runs this scan. */
import{createHash}from"node:crypto";
import{parseManifest,parseSessionKey,objectKey,type ObjectStore}from"../../packages/object-store/src/index.ts";
import{readCapped}from"../../packages/pipeline/src/lib/pipeline/adapter.ts";
import{canonicalManifestIdentity,manifestUserId}from"../shared/manifest-identity.ts";
import{layoutComponent}from"../shared/layout-component.ts";
import{streamManifestRecordLines}from"../session-read-api/canonical-projection.ts";
import{boundedObjectStore}from"../shared/bounded-object-store.ts";
import{nodeReadBudget}from"../session-read-api/node-read-profile.ts";
import type{SemanticBoundedSql}from"../search-features/semantic-copy-postgres.ts";
const sha=(v:Uint8Array|string)=>createHash("sha256").update(v).digest("hex"),HEX=/^[a-f0-9]{64}$/,UUID=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/,DEC=/^(0|[1-9][0-9]*)$/;
function no(code:string):never{throw Object.assign(Error(code),{code})}
export interface HistoryInventoryTicket{carrierSha256:string;physicalRoot:string;bucket:string;scanToken:string;leaseToken:string;epoch:string;referenceCount:string;leaseUntil:string}
export interface HistoryInventoryRecord{orgId:string;workspaceId:string;originId:string;recordId:string;revision:string;count:number}
export interface HistoryInventoryRef{ordinal:string;pointer:Record<string,unknown>;pointerSha256:string}
export interface HistoryInventoryRpc{
 claim(signal:AbortSignal):Promise<HistoryInventoryTicket|null>;
 refs(t:HistoryInventoryTicket,after:string,signal:AbortSignal):Promise<{references:HistoryInventoryRef[];total:string;next:string|null}>;
 page(t:HistoryInventoryTicket,page:string,records:HistoryInventoryRecord[],signal:AbortSignal):Promise<void>;
 finish(t:HistoryInventoryTicket,pages:string,records:string,descriptorSha256:string,signal:AbortSignal):Promise<void>;
 heartbeat(t:HistoryInventoryTicket,signal:AbortSignal):Promise<void>;
}
export function historyInventoryRpc(db:SemanticBoundedSql,target:{generation:string;bucket:string},now=Date.now):HistoryInventoryRpc{
 if(!HEX.test(target.generation)||!target.bucket)no("history_inventory_target");
 const call=async(fn:string,p:Record<string,unknown>,signal:AbortSignal)=>{
  const r=await db.queryBounded<any>("SELECT sessions.ingest_history_inventory_"+fn+"($1::jsonb)AS result",[JSON.stringify({...p,generation:target.generation})],{signal,deadline:now()+15000,now});
  if(r.rows.length!==1||!r.rows[0]?.result||typeof r.rows[0].result!=="object")no("history_inventory_rpc");return r.rows[0].result;
 };
 const args=(t:HistoryInventoryTicket)=>({carrierSha256:t.carrierSha256,scanToken:t.scanToken,leaseToken:t.leaseToken,epoch:t.epoch});
 return{
  async claim(signal){const r=await call("claim",{bucket:target.bucket},signal);if(r.ticket===null)return null;const t=r.ticket;if(!t||!HEX.test(t.carrierSha256)||!UUID.test(t.scanToken)||!UUID.test(t.leaseToken)||!DEC.test(t.epoch)||!DEC.test(t.referenceCount)||BigInt(t.referenceCount)<1n||t.bucket!==target.bucket||typeof t.physicalRoot!=="string"||!t.physicalRoot.endsWith("/")||!Number.isFinite(Date.parse(t.leaseUntil)))no("history_inventory_ticket");return t;},
  async refs(t,after,signal){const r=await call("refs",{...args(t),after},signal);if(r.total!==t.referenceCount||!Array.isArray(r.references)||r.references.length>100||!(r.next===null||typeof r.next==="string"&&DEC.test(r.next)))no("history_inventory_refs");return r;},
  async page(t,page,records,signal){if((await call("page",{...args(t),page,records},signal)).stored!==true)no("history_inventory_page");},
  async finish(t,pages,records,descriptorSha256,signal){if((await call("finish",{...args(t),pages,records,descriptorSha256},signal)).complete!==true)no("history_inventory_finish");},
  async heartbeat(t,signal){const r=await call("heartbeat",args(t),signal);if(!Number.isFinite(Date.parse(r.leaseUntil)))no("history_inventory_heartbeat");t.leaseUntil=r.leaseUntil;},
 };
}
async function scanHistoryCarrierBody(t:HistoryInventoryTicket,deps:{rpc:HistoryInventoryRpc;store:ObjectStore;keyRoot:string;decompress:(s:Uint8Array,n:number)=>Uint8Array},signal:AbortSignal){
 const store=boundedObjectStore(deps.store,signal),seen=new Map<string,{manifest:any;at:NonNullable<ReturnType<typeof parseSessionKey>>;pointer:Record<string,unknown>}>(),descriptor=createHash("sha256"),budget=nodeReadBudget("large");
 let cursor="0",refCount=0n,records=0n,page=0n,batch:HistoryInventoryRecord[]=[],rawBytes=0,manifestBytes=0;
 const flush=async()=>{if(!batch.length)return;await deps.rpc.page(t,String(page++),batch,signal);batch=[];};
 while(true){
  const result=await deps.rpc.refs(t,cursor,signal);
  for(const ref of result.references){
   signal.throwIfAborted();if(ref.ordinal!==String(refCount++)||!HEX.test(ref.pointerSha256))no("history_inventory_reference_order");
   const p=ref.pointer,key=p.manifest_key;if(typeof key!=="string"||!key.startsWith(t.physicalRoot)||p.storage_provider!=="s3"||p.bucket!==t.bucket)no("history_inventory_reference_identity");
   const at=parseSessionKey(key);if(!at||key.slice(0,key.length-at.rest.length)!==t.physicalRoot||!at.prefix||typeof p.org_id!=="string"||typeof p.workspace_id!=="string"||typeof p.user_id!=="string"||!p.user_id)no("history_inventory_pointer");
   // A generation prefix still binds the exact original org namespace.
   const base=at.prefix.replace(/\/upload-generations\/g=[a-f0-9-]+$/,"");if(base!==deps.keyRoot+"/"+p.org_id)no("history_inventory_namespace");
   let source=seen.get(key);
   if(!source){
    const object=await store.getObject(objectKey(key)),bytes=await readCapped(object.body,16*1024**2);signal.throwIfAborted();if(!bytes)no("history_inventory_manifest_bound");
    manifestBytes+=bytes.length;if(manifestBytes>budget.sourceBytes)no("history_inventory_metadata_bound");
    const manifest=await parseManifest(new TextDecoder("utf-8",{fatal:true}).decode(bytes),key);source={manifest,at,pointer:p};seen.set(key,source);
   }
   const m=source.manifest,identity=canonicalManifestIdentity(m);
   if(m.session.workspace_id!==layoutComponent(p.workspace_id)||m.session.harness!==p.harness||m.session.session_id!==p.session_id||m.session.machine_id!==p.machine_id||manifestUserId(m)!==p.user_id||
    identity.sessionId!==(p.canonical_session_id??p.session_id)||identity.machineId!==(p.canonical_machine_id??p.machine_id)||String(m.revision)!==String(p.revision)||m.raw.content_sha256!==String(p.content_sha256).replace(/^\\x/,"")||m.raw.format!=="ndjson"||!m.raw.scrub)no("history_inventory_manifest_authority");
   descriptor.update(JSON.stringify([ref.ordinal,ref.pointerSha256,key,m.raw.content_sha256,String(m.revision)])+"\n");
  }
  if(result.next===null)break;if(!result.references.length||BigInt(result.next)<=BigInt(cursor))no("history_inventory_reference_cursor");cursor=result.next;
 }
 if(refCount!==BigInt(t.referenceCount))no("history_inventory_reference_count");
 // Validation above covers ALL owners/references before any location metadata is
 // admitted. Invalid foreign references are a refusal, never filtered absence.
 for(const [key,{manifest,at,pointer:owner}]of seen){
  const ownerRef={orgId:String(owner.org_id),workspaceId:String(owner.workspace_id)};
  const remainingBytes=budget.sourceBytes-rawBytes-manifestBytes,remainingRecords=budget.sourceRecords-Number(records);
  if(remainingBytes<=0||remainingRecords<0)no("history_inventory_source_bound");
  for await(const line of streamManifestRecordLines(manifest,store,at.prefix!,deps.decompress,{signal,maxRawBytes:remainingBytes,maxRecords:remainingRecords})){
   if(records>=BigInt(budget.sourceRecords))no("history_inventory_record_bound");
   batch.push({...ownerRef,originId:line.record.origin_id,recordId:line.record.record_id,revision:line.record.revision,count:1});records++;
   if(batch.length===500)await flush();
  }
  rawBytes+=manifest.raw.chunks.reduce((n:any,c:any)=>n+(c.stored_raw_bytes??c.bytes),0);if(rawBytes>budget.sourceBytes)no("history_inventory_source_bound");
 }
 await flush();await deps.rpc.finish(t,String(page),String(records),descriptor.digest("hex"),signal);
 return{references:String(refCount),records:String(records),pages:String(page),manifests:seen.size,sourceBytes:rawBytes};
}

/** Heartbeats share the exact immutable scan lease; stop/join BEFORE finish. */
export async function scanHistoryCarrier(t:HistoryInventoryTicket,deps:Parameters<typeof scanHistoryCarrierBody>[1],parentSignal:AbortSignal,options:{heartbeatMs?:number;maxJobMs?:number}={}){
 const interval=options.heartbeatMs??30000,budget=options.maxJobMs??20*60*1000;
 if(!Number.isInteger(interval)||interval<10||interval>60000||!Number.isInteger(budget)||budget<1000||budget>60*60*1000)no("history_inventory_job_bounds");
 const controller=new AbortController(),signal=AbortSignal.any([parentSignal,controller.signal,AbortSignal.timeout(budget)]);let heartbeat:Promise<void>|undefined,failed:unknown,finishing=false;
 const timer=setInterval(()=>{if(heartbeat||finishing||signal.aborted)return;heartbeat=deps.rpc.heartbeat(t,signal).catch(error=>{failed=error;controller.abort(error)}).finally(()=>{heartbeat=undefined})},interval);
 const rpc={...deps.rpc,async finish(...args:Parameters<HistoryInventoryRpc["finish"]>){finishing=true;clearInterval(timer);await heartbeat;if(failed)throw failed;signal.throwIfAborted();await deps.rpc.heartbeat(t,signal);await deps.rpc.finish(...args)}};
 try{return await scanHistoryCarrierBody(t,{...deps,rpc},signal)}finally{finishing=true;clearInterval(timer);controller.abort(Error("history_inventory_complete"));await heartbeat}
}
