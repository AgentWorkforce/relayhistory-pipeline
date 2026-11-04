/** Test-only stand-in for the RHC PostgreSQL journal, never production fallback.
 * Actual caller integration must use the source journal's stored version/digest.
 */
import type {Ledger,LedgerTenant} from "../ledger.ts";
import type {OrderedControl} from "../control-protocol.ts";
export function orderedFixtureLedger(base:Ledger,db:{query(s:string,p?:any[]):Promise<{rows:any[]}>},epoch="3".repeat(64)):Ledger{
 async function next(tenant:LedgerTenant,kind:OrderedControl["kind"],payload:unknown):Promise<OrderedControl>{
  const r=await db.query("SELECT coalesce((SELECT max(version) FROM sessions.ingest_control_receipts WHERE org_id=$2 AND workspace_id=$3 AND user_id=$4 AND epoch=$1),0)::text AS v,encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,$5::text,$6::jsonb)::text,'UTF8')),'hex') AS d",[epoch,tenant.orgId,tenant.workspaceId,tenant.userId,kind,JSON.stringify(payload)]);
  return{epoch,version:(BigInt(r.rows[0].v)+1n).toString(),digest:r.rows[0].d,kind};
 }
 return{...base,async setSessionUploads(input){
  const c=await next(input.tenant,"selection",{origin_id:input.originId,sessions:input.sessions.map(([source,session_id])=>({source,session_id})),enabled:input.enabled});
  await base.applyOrderedControl!({...input,control:{...c,kind:"selection"}});
 },async resetUploadedSessions(tenant,requestId){
  const c=await next(tenant,"reset",{request_id:requestId});
  return(await base.applyOrderedControl!({tenant,requestId,control:{...c,kind:"reset"}})).deleted!;
 }};
}

/** Synthetic owner attestation only; production must use the real two-source exporter. */
export function fixtureSeatBaseline(generation:string,epoch="3".repeat(64),floor="9007199254740993"){
 return{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",
 sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:floor,isCalled:true,incrementBy:"1",cycle:false})),
 allocatedHighWater:floor,generation,sourceIdentitySha256:"c".repeat(64),sourceProofSha256:"d".repeat(64)};
}
export async function initializeFixtureSeatClock(db:{query(s:string,p?:any[]):Promise<unknown>},generation:string,epoch="3".repeat(64)){
 await db.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify(fixtureSeatBaseline(generation,epoch))]);
}
