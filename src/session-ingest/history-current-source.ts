/** Complete multi-lookup source capture. This is legacy metadata evidence only:
 * initialized target550 custody overrides it before any mask/body selection. */
import type { NodeReadProfile } from "../session-read-api/node-read-profile.ts";
import { nodeReadBudget } from "../session-read-api/node-read-profile.ts";
import {
 HISTORY_CUSTODY_RECORD_IDS, type HistoryCustodyLookupInput, type HistoryCustodySourcePort,
 type HistoryCustodyRow, type HistoryCustodyOwner, type HistoryCustodyOwnerHead,
 type HistoryCustodyTerminal,
} from "./history-custody-contract.ts";
export interface HistorySourceMetadataCapture {
 readonly input: Omit<HistoryCustodyLookupInput,"recordIds"|"priorOwners">;
 readonly recordIds: readonly string[];
 readonly rows: readonly HistoryCustodyRow[];
 readonly owners: ReadonlyMap<string,HistoryCustodyOwner>;
 readonly lookups: readonly {recordIds:readonly string[];lookupSha256:string;terminal:HistoryCustodyTerminal}[];
 readonly bytes:number;
}
const hash=(v:unknown):v is string=>typeof v==="string"&&/^[a-f0-9]{64}$/.test(v);
const cmp=(a:string,b:string)=>Buffer.compare(Buffer.from(a),Buffer.from(b));
function fail(code:string):never{throw Object.assign(Error(code),{code})}
const head=(v:HistoryCustodyOwnerHead)=>JSON.stringify([v.userId,v.controlEpoch,v.version,v.pending,v.privacyVersion,v.pendingControl===null?null:[v.pendingControl.epoch,v.pendingControl.version,v.pendingControl.digest,v.pendingControl.kind]]);
export async function captureHistoryCurrentMetadata(
 port:HistoryCustodySourcePort,input:Omit<HistoryCustodyLookupInput,"recordIds"|"priorOwners">,
 recordIds:Iterable<string>,priorOwners:Iterable<string>,
 options:{profile?:NodeReadProfile;signal?:AbortSignal}={},
):Promise<HistorySourceMetadataCapture>{
 const budget=nodeReadBudget(options.profile),ids=[...new Set(recordIds)].sort(cmp),known=new Set(priorOwners);
 if(!ids.length)fail("history_source_no_record_keys");
 if(ids.length>budget.sourceRecords||known.size>budget.sourceRecords)fail("history_source_metadata_bound");
 const owners=new Map<string,HistoryCustodyOwner>(),rows:HistoryCustodyRow[]=[],lookups:HistorySourceMetadataCapture["lookups"][number][]=[];
 const identities=new Set<string>();let bytes=0;
 for(let at=0;at<ids.length;at+=HISTORY_CUSTODY_RECORD_IDS){
  options.signal?.throwIfAborted();const requested=ids.slice(at,at+HISTORY_CUSTODY_RECORD_IDS),prior=[...known].sort(cmp);
  const page=await port.snapshot({...input,recordIds:requested,priorOwners:prior},options.signal);options.signal?.throwIfAborted();
  if(page.complete!==true||page.orgId!==input.orgId||page.workspaceId!==input.workspaceId||page.controlEpoch!==input.controlEpoch||page.sourceAuthoritySha256!==input.expectedSourceAuthoritySha256||page.providerIdentitySha256!==input.expectedProviderIdentitySha256||!hash(page.lookupSha256)||JSON.stringify([...page.requestedRecordIds].sort(cmp))!==JSON.stringify(requested)||page.terminal.rowCount!==page.rows.length||page.terminal.ownerCount!==page.owners.length)fail("history_source_metadata_incomplete");
  bytes+=Buffer.byteLength(JSON.stringify(page));if(bytes>budget.sourceBytes||rows.length+page.rows.length>budget.sourceRecords)fail("history_source_metadata_bound");
  const pageOwners=new Map<string,HistoryCustodyOwner>();
  for(const owner of page.owners){
   if(pageOwners.has(owner.userId)||owner.controlEpoch!==input.controlEpoch||owner.pending)fail("history_source_owner_changed");
   pageOwners.set(owner.userId,owner);
   const previous=owners.get(owner.userId);
   if(previous&&(head(previous)!==head(owner)||JSON.stringify(previous.privacy)!==JSON.stringify(owner.privacy)))fail("history_source_owner_changed");
  }
  if(prior.some(id=>!pageOwners.has(id))||page.rows.some(row=>!pageOwners.has(row.userId)||!requested.includes(row.recordId)))fail("history_source_owner_incomplete");
  for(const row of page.rows){
   const id=JSON.stringify([row.originId,row.recordId]);if(identities.has(id))fail("history_source_duplicate_native");
   identities.add(id);rows.push(row);
  }
  for(const [id,owner]of pageOwners){known.add(id);if(!owners.has(id))owners.set(id,Object.freeze({...owner}));}
  if(known.size>budget.sourceRecords)fail("history_source_metadata_bound");
  lookups.push({recordIds:Object.freeze(requested),lookupSha256:page.lookupSha256,terminal:Object.freeze({...page.terminal})});
 }
 return Object.freeze({input:Object.freeze({...input}),recordIds:Object.freeze(ids),rows:Object.freeze(rows),owners,lookups:Object.freeze(lookups),bytes});
}
/** After all body/S3 work, before target CAS. Never substitutes a newer context.
 * Target must still lock/check the same owners and native lineage in final CAS. */
export async function revalidateHistoryCurrentMetadata(port:HistoryCustodySourcePort,captured:HistorySourceMetadataCapture,signal?:AbortSignal):Promise<string>{
 signal?.throwIfAborted();const users=[...captured.owners.keys()].sort(cmp);
 const r=await port.revalidate({...captured.input,owners:users},signal);signal?.throwIfAborted();
 if(r.complete!==true||r.controlEpoch!==captured.input.controlEpoch||r.sourceAuthoritySha256!==captured.input.expectedSourceAuthoritySha256||r.providerIdentitySha256!==captured.input.expectedProviderIdentitySha256||!hash(r.lookupSha256)||r.owners.length!==users.length)fail("history_source_owner_incomplete");
 const seen=new Set<string>();
 for(const owner of r.owners){
  const original=captured.owners.get(owner.userId);
  if(seen.has(owner.userId)||!original||owner.pending||head(owner)!==head(original))fail("history_source_owner_changed");
  seen.add(owner.userId);
 }
 if(users.some(user=>!seen.has(user)))fail("history_source_owner_incomplete");
 return r.lookupSha256;
}
