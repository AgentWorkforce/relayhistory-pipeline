/**
 * TEST-ONLY support for the ledger v8 Phase 1 prototype (packed ACK vectors).
 * Builds a disposable database (PGlite, or a fresh database on an owned disposable PG17
 * container via ACK_V2_TEST_PG_URL), applies supabase/migrations unchanged, optionally the
 * test-only fixture SQL, and drives the REAL delivery path (deliverBatch -> SQL RPCs).
 */
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import pg from "pg";
import {createHash} from "node:crypto";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {prepareDelivery,deliveryAccount} from "../vendor/delivery/prepare.ts";
import type {CurrentCustodyPacket,SourceCurrentRecord} from "../custody-protocol.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";

export const root=join(import.meta.dir,"../../..");
export const FIXTURE=process.env.ACK_V2_FIXTURE??"src/session-ingest/__tests__/fixtures/ack-v2-prototype.sql";
export const generation="a".repeat(64),epoch="3".repeat(64),IDENT="c".repeat(64);
const read=(p:string)=>readFileSync(join(root,p),"utf8");
export interface Q{exec(s:string):Promise<unknown>;query(s:string,p?:unknown[]):Promise<{rows:any[]}>;close():Promise<void>}
export type Mode="baseline"|"bridge-v1"|"bridge-v2";

export async function openDb(kind:"pglite"|"pg"):Promise<Q>{
 if(kind==="pglite"){const db=new PGlite({extensions:{btree_gin,pg_trgm}});
  return{exec:s=>db.exec(s),query:(s,p)=>db.query(s,p as never) as never,close:()=>db.close()}}
 const url=process.env.ACK_V2_TEST_PG_URL;if(!url)throw Error("ACK_V2_TEST_PG_URL unset");
 const admin=new pg.Client({connectionString:url});await admin.connect();
 const name="ackv2_"+crypto.randomUUID().replaceAll("-","");await admin.query("CREATE DATABASE "+name);
 const u=new URL(url);u.pathname="/"+name;const c=new pg.Client({connectionString:u.toString()});await c.connect();
 return{url:u.toString(),exec:s=>c.query(s),query:(s,p)=>c.query(s,p as never) as never,
  close:async()=>{await c.end();await admin.query("DROP DATABASE IF EXISTS "+name+" WITH (FORCE)").catch(()=>{});await admin.end()}} as Q&{url:string};
}

export async function fixture(kind:"pglite"|"pg",mode:Mode,opts:{highWater?:string;beforeFixture?:(url:string|undefined)=>Promise<void>}={}){
 const db=await openDb(kind);
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await db.exec(read("supabase/bootstrap/"+f+".sql"));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+generation+"','pass')");
 const owner=async(name:string,p:unknown)=>(await db.query("SELECT sessions."+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0]!.r as any;
 const hw=opts.highWater??"9007199254740993";
 await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation,sourceIdentitySha256:IDENT,controlEpoch:epoch,privacyBaselineSha256:"a".repeat(64),retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
 await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:hw,isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:hw,generation,sourceIdentitySha256:IDENT,sourceProofSha256:"d".repeat(64)});
 if(opts.beforeFixture)await opts.beforeFixture((db as any).url);
 if(mode!=="baseline")await db.exec(read(FIXTURE));
 if(mode==="bridge-v2")await db.query("SELECT sessions.ingest_ack_format_set('v2')");
 let lastSql:string|null=null;
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p);await db.exec("COMMIT");return r as never}catch(e:any){lastSql=String(e?.code??"")+" "+String(e?.message??e);await db.exec("ROLLBACK");throw e}};
 const lastSqlError=()=>{const v=lastSql;lastSql=null;return v};
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation});
 const fs=scratchStore();const deps={ledger,acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}};
 const sign=async(witness:any):Promise<CurrentCustodyPacket>=>{const r=await db.query("SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex') AS d",[JSON.stringify(witness)]);return{witness,sourceDigest:r.rows[0]!.d as string}};
 async function version(tenant=TENANT){return String(BigInt((await db.query("SELECT coalesce(max(version),0)::text AS n FROM sessions.ingest_control_receipts WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3",[tenant.orgId,tenant.workspaceId,tenant.userId])).rows[0]!.n as string))}
 /** A sender bound to its own connection (for concurrency tests). */
 function senderOn(q:{query(s:string,p?:unknown[]):Promise<any>},onTx?:(sql:string,ms:number,ok:boolean)=>void){
  const r2=async(s:string,p:unknown[]=[])=>{const t0=performance.now();await q.query("BEGIN");await q.query("SET LOCAL ROLE bulls96_session_ingest");
   try{const r=await q.query(s,p);await q.query("COMMIT");onTx?.(s,performance.now()-t0,true);return r as never}catch(e){await q.query("ROLLBACK");onTx?.(s,performance.now()-t0,false);throw e}};
  const l2=sqlLedger(r2,{acceptanceMode:"buffered",expectedGeneration:generation});
  return(id:string,records:any[],current:Record<string,SourceCurrentRecord|null>={},tenant=TENANT)=>send(id,records,current,tenant,{...deps,ledger:l2});
 }
 /** Send one batch of many records; `current` maps record_id -> source current metadata (legacy custody). */
 async function send(id:string,records:any[],current:Record<string,SourceCurrentRecord|null>={},tenant=TENANT,via=deps){
  const raw=await batch(records,{batch_id:id,account_id:await deliveryAccount(tenant)});const p=await prepareDelivery(tenant,raw);const v=await version(tenant);
  const witness:any={version:1,issuer:"relayhistory-cloud",sourceIdentitySha256:IDENT,controlEpoch:epoch,privacyBaselineSha256:"a".repeat(64),tenant,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,sourceControlVersion:v,pending:false,pendingControl:null,privacyVersion:v,originalRecordCount:raw.records.length,candidateCount:p.records.length,
   records:p.records.map((r,i)=>({preparedOrd:i,processingRank:i,candidateDigest:r.digest,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateFenced:false,currentFenced:false,current:current[r.record_id]??null})),privacy:v==="0"?{complete:true,deletions:[],resets:[]}:null};
  return deliverBatch({...via,custody:await sign(witness)},tenant,raw);
 }
 async function control(kind:"selection"|"reset",payload:any,tenant=TENANT){
  const v=String(BigInt(await version(tenant))+1n);
  const digest=(await db.query("SELECT encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,$5::text,$6::jsonb)::text,'UTF8')),'hex') AS d",[epoch,tenant.orgId,tenant.workspaceId,tenant.userId,kind,JSON.stringify(payload)])).rows[0]!.d as string;
  const control={epoch,version:v,digest,kind};
  const custody=await sign({version:1,issuer:"relayhistory-cloud",sourceIdentitySha256:IDENT,controlEpoch:epoch,privacyBaselineSha256:"a".repeat(64),tenant,originId:kind==="selection"?payload.origin_id:null,batchId:null,batchDigest:null,sourceControlVersion:v,pending:true,pendingControl:control,privacyVersion:String(BigInt(v)-1n),originalRecordCount:0,candidateCount:0,records:[],privacy:v==="1"?{complete:true,deletions:[],resets:[]}:null});
  const input=kind==="selection"?{tenant,originId:payload.origin_id,sessions:payload.sessions.map((x:any)=>[x.source,x.session_id]),enabled:payload.enabled,control:{...control,kind:"selection" as const},custody}:{tenant,requestId:payload.request_id,control:{...control,kind:"reset" as const},custody};
  return ledger.applyOrderedControl!(input);
 }
 const seats=async(id:string)=>(await db.query("SELECT sessions.ingest_receipt_seats_for_stage(o.active_stage_token) AS r FROM sessions.ingest_buffer_outbox o WHERE batch_id=$1",[id])).rows[0]?.r as any;
 const gcOrdinals=async()=>(await db.query("SELECT o.batch_id,sessions.ingest_buffer_gc_ordinals(o.active_stage_token) AS k FROM sessions.ingest_buffer_outbox o WHERE o.state='buffered' ORDER BY o.batch_id")).rows;
 /** Format-independent semantic slot projection: v1 seat key and v2 seat both resolve to the numeric seat. */
 const slots=async()=>(await db.query(`SELECT n.key_sha256,n.lineage::text,n.current_owner,n.current_source,n.current_session,n.current_revision::text,n.current_digest,
  n.current_reset_epoch::text,n.current_fence_epoch::text,coalesce((to_jsonb(n)->>'current_seat'),s.seat::text) AS seat,n.current_legacy_key
  FROM sessions.ingest_lazy_native_slots n LEFT JOIN sessions.ingest_receipt_seats s ON s.key_sha256=n.current_seat_key ORDER BY 1`)).rows;
 const close=async()=>{fs.done();await db.close()};
 return{db,owner,ledger,deps,send,senderOn,control,seats,gcOrdinals,slots,close,sign,lastSqlError,url:(db as any).url as string|undefined};
}
export type F=Awaited<ReturnType<typeof fixture>>;
export {TENANT,record};
export async function meta(rec:any,tenant=TENANT):Promise<SourceCurrentRecord>{const p=await prepareDelivery(tenant,await batch([rec],{account_id:await deliveryAccount(tenant)})),r=p.records[0]!;
 return{userId:tenant.userId,revision:String(r.revision),digest:r.digest,revisionId:r.revision_id,kind:r.kind,source:r.source,sessionId:r.session_id,operation:r.operation,catalogAcceptedSeq:null,projectionAcceptedSeq:"17"} as SourceCurrentRecord}

/** §9.4 preimage in TypeScript, for golden-vector equality with SQL (P13). */
export function preimageField(tag:number,v:Buffer|null){if(v===null)return Buffer.from([0,0,0,0,0]);const h=Buffer.alloc(5);h[0]=tag;h.writeUInt32BE(v.length,1);return Buffer.concat([h,v])}
export const ft=(s:string|null)=>preimageField(1,s===null?null:Buffer.from(s,"utf8"));
export const fi=(n:string|null)=>{if(n===null)return preimageField(2,null);const b=Buffer.alloc(8);b.writeBigInt64BE(BigInt(n));return preimageField(2,b)};
export const fu=(u:string)=>preimageField(3,Buffer.from(u.replaceAll("-",""),"hex"));
export const fj=(pgJsonText:string|null)=>preimageField(4,pgJsonText===null?null:Buffer.from(pgJsonText,"utf8"));
export function attemptSha(a:any){return createHash("sha256").update(Buffer.concat([Buffer.from("96bulls/ack-attempt/v1\0","utf8"),fu(a.claim_token),ft(a.org_id),ft(a.workspace_id),ft(a.user_id),ft(a.origin_id),ft(a.batch_id),ft(a.digest),fi(a.reset_epoch),ft(a.schema_generation),fi(a.source_control_version),ft(a.source_identity_sha256),ft(a.source_witness_sha256),fj(a.records),fj(a.epochs),fj(a.source_processing_ranks)])).digest("hex")}
