/** Retained physical records and currently visible logical winners are distinct.
 * This module consumes QUALIFIED current custody, never an unqualified raw MAX.
 * The cross-origin choice is the same exported canonical fold, not a new decoder.
 */
import{createHash}from"node:crypto";
import{newestRelayhistoryAcrossOrigins,relayhistoryRevisionContent,isLegacyRelayhistoryRecord,type RelayhistoryStreamRecord}from"../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import{CONVERGENCE_EVENT_KIND}from"../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
export interface HistoryScope{orgId:string;workspaceId:string;source:string;sessionId:string}
export interface HistoryCurrentSeat{
 originId:string;recordId:string;kind:string;revision:string;acceptedSeq:string;operation:"upsert"|"delete";
 userId:string;source:string;sessionId:string|null;contentSha256:string;lineage:string;
}
export interface HistoryLogicalMask{
 scope:HistoryScope;source:{epoch:string;fingerprint:string};
 /** Includes explicit null for erased keys. Omission is UNKNOWN, not absence. */
 winners:ReadonlyMap<string,Readonly<HistoryCurrentSeat>|null>;
}
const HEX=/^[a-f0-9]{64}$/,DEC=/^(0|[1-9][0-9]*)$/,MAX=9223372036854775807n;
function no(code:string):never{throw Object.assign(Error(code),{code})}
export const historyRecordContentSha256=(r:RelayhistoryStreamRecord)=>createHash("sha256").update(relayhistoryRevisionContent(r)).digest("hex");
function checked(s:HistoryCurrentSeat){
 if(!s.originId||!s.recordId||!s.kind||!s.userId||!s.source||!HEX.test(s.contentSha256)||!DEC.test(s.revision)||BigInt(s.revision)<1n||BigInt(s.revision)>MAX||!DEC.test(s.acceptedSeq)||BigInt(s.acceptedSeq)<1n||BigInt(s.acceptedSeq)>MAX||!DEC.test(s.lineage)||!(s.sessionId===null||typeof s.sessionId==="string"&&s.sessionId.length>0)||!["upsert","delete"].includes(s.operation))no("history_visibility_seat");
}
/** Input is exactly one CURRENT lifecycle per native(origin,record). Historical
 * S3 versions must never be offered here as competing current custody. */
export function chooseHistoryCurrentWinners(current:Iterable<HistoryCurrentSeat>):Map<string,Readonly<HistoryCurrentSeat>>{
 const native=new Set<string>(),seats=new Map<string,string>(),wrapped:{kind:string;record_id:string;accepted_seq:string;seat:HistoryCurrentSeat}[]=[];
 for(const s of current){checked(s);const key=JSON.stringify([s.originId,s.recordId]);if(native.has(key))no("history_visibility_duplicate_current");native.add(key);const seatKey=JSON.stringify([s.recordId,s.acceptedSeq]);if(seats.has(seatKey)&&seats.get(seatKey)!==key)no("history_visibility_acceptance_alias");seats.set(seatKey,key);wrapped.push({kind:s.kind,record_id:s.recordId,accepted_seq:s.acceptedSeq,seat:{...s}})}
 const selected=newestRelayhistoryAcrossOrigins(wrapped),result=new Map<string,Readonly<HistoryCurrentSeat>>();
 for(const w of selected){if(result.has(w.record_id))no("history_visibility_independent_domain");result.set(w.record_id,Object.freeze(w.seat))}return result;
}
/** BEFORE normalize/project. A copy physically retained for fallback is hidden
 * while another origin wins elsewhere. Raw bytes remain unchanged for rewrite. */
export function historyRecordVisible(record:RelayhistoryStreamRecord,userId:string,mask:HistoryLogicalMask):boolean{
 if(record.kind===CONVERGENCE_EVENT_KIND||isLegacyRelayhistoryRecord(record))return true;
 if(!HEX.test(mask.source.fingerprint)||!DEC.test(mask.source.epoch)||BigInt(mask.source.epoch)<1n)no("history_visibility_source");
 if(!mask.winners.has(record.record_id))no("history_visibility_unknown");
 const w=mask.winners.get(record.record_id);if(!w)return false;checked(w);
 if(w.originId!==record.origin_id||w.revision!==record.revision||w.acceptedSeq!==record.accepted_seq||w.source!==mask.scope.source||w.sessionId!==mask.scope.sessionId||w.userId!==userId)return false;
 if(w.kind!==record.kind||w.operation!==record.operation||w.contentSha256!==historyRecordContentSha256(record))no("history_visibility_body_mismatch");
 return true;
}


/** A metadata mask cannot turn a missing current body into an empty success.
 * Hidden other-scope/NULL/tombstone winners do not require a visible upsert. */
export function assertHistoryMaskCoverage(mask:HistoryLogicalMask,visible:Iterable<RelayhistoryStreamRecord>):void{
 const seen=new Set<string>();for(const record of visible)
  if(record.kind!==CONVERGENCE_EVENT_KIND&&!isLegacyRelayhistoryRecord(record)&&record.operation==="upsert")seen.add(record.record_id);
 for(const [recordId,winner]of mask.winners)
  if(winner&&winner.operation==="upsert"&&winner.source===mask.scope.source&&winner.sessionId===mask.scope.sessionId&&!seen.has(recordId))
   no("history_visibility_missing_body");
}
