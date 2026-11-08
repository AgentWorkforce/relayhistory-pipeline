/**
 * Zero-503 schema windows (migration 20261105043885, docs/design/zero-503-schema-windows.md), on PGlite over
 * EVERY migration: the bounded overlap keeps an old Worker admitting while readiness moves to the next
 * generation, a Worker at the next generation admits after the rebind, the old one is refused once the window
 * closes or expires, and the window is owner-only, explicit, append-only and never a downgrade.
 * The same protocol on real PostgreSQL: generation-overlap.postgres.test.ts.
 */
import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {prepareDelivery} from "../vendor/delivery/prepare.ts";
import {rebindIngestGeneration} from "../../hoopdb/ingest-generation-rebind.ts";
import {openGenerationOverlap,closeGenerationOverlap,generationOverlapStatus} from "../../hoopdb/ingest-generation-overlap.ts";
import {TENANT,batch,record,scratchStore} from "./support.ts";
import {SESSION_BOOTSTRAP_FILES} from "../../hoopdb/session-bootstrap-chain.ts";
setDefaultTimeout(60000);
const root=join(import.meta.dir,"../../.."),A="a".repeat(64),B="f".repeat(64),C="e".repeat(64),APPROVAL="9".repeat(64);
const epoch="3".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});

async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 for(const f of SESSION_BOOTSTRAP_FILES)await db.exec(read(f));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+A+"','pass')");
 const owner=async(name:string,p:unknown)=>(await db.query("SELECT sessions."+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0]!.r as any;
 await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation:A,sourceIdentitySha256:identity,controlEpoch:epoch,privacyBaselineSha256:baseline,retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
 await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:"9007199254740993",isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:"9007199254740993",generation:A,sourceIdentitySha256:identity,sourceProofSha256:"d".repeat(64)});
 await db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",[identity,epoch,baseline,"e".repeat(64)]);
 const as=(role:string)=>async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE "+role);try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 const run=as("bulls96_session_ingest");
 const fs=scratchStore();open.push(fs.done);
 const worker=(expected:string,accept?:string[])=>({ledger:sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:expected,...(accept?{acceptGenerations:accept}:{})}),acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}});
 const rpc=async(fn:string,p:unknown)=>((await run("SELECT sessions."+fn+"($1::jsonb) AS r",[JSON.stringify(p)])) as any).rows[0].r;
 let n=0;
 async function upload(deps:ReturnType<typeof worker>){
  const raw=await batch([record({record_id:"r"+(++n)})],{batch_id:"b"+n});const p=await prepareDelivery(TENANT,raw);
  const custody=await rpc("ingest_frozen_source_witness",{kind:"batch",tenant:TENANT,originId:p.originId,batchId:p.batchId,batchDigest:p.batchDigest,originalRecordCount:raw.records.length,
   candidates:p.records.map((r,i)=>({preparedOrd:i,recordId:r.record_id,revision:String(r.revision),revisionId:r.revision_id,candidateDigest:r.digest,source:r.source,sessionId:r.session_id}))});
  return deliverBatch({...deps,custody},TENANT,raw);
 }
 const ok=async(deps:ReturnType<typeof worker>)=>{const r=await upload(deps);expect(r.custodyAck?.sourceDigest).toMatch(/^[a-f0-9]{64}$/);};
 const refused=async(deps:ReturnType<typeof worker>)=>{let e:unknown=null;try{await upload(deps)}catch(x){e=x}expect(e).not.toBeNull();const x=e as {code?:string;cause_code?:string;status?:number};return {code:x.cause_code??x.code,status:x.status};};
 const client={query:async(text:string,params?:unknown[])=>(params?.length?db.query(text,params as never):db.query(text)) as never};
 const marker=(generation:string,status:"pending"|"pass")=>db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status=$2",[generation,status]);
 return{db,owner,run,as,worker,upload,ok,refused,client,marker};
}
const err=async(p:Promise<unknown>)=>{try{await p;return null}catch(e){return String((e as Error).message)}};
const code=async(p:Promise<unknown>)=>{try{await p;return null}catch(e){return (e as {code?:string}).code??"?"}};

test("status quo without a window: from the pending write until the rebind every upload is refused (the outage)",async()=>{
 const f=await fixture();
 await f.ok(f.worker(A));
 await f.marker(B,"pending");
 expect((await f.refused(f.worker(A))).code).toBe("ledger_sqlstate_55000");
 expect((await f.refused(f.worker(B))).code).toBe("ledger_sqlstate_55000");
 await f.marker(B,"pass");
 expect((await f.refused(f.worker(A))).code).toBe("ledger_sqlstate_55000");
 // A Worker at B is refused too until the rebind moves the policy and admissions.
 expect((await f.refused(f.worker(B))).code).toBe("ledger_sqlstate_55000");
 expect((await f.refused(f.worker(B,[B,A]))).code).toBe("ledger_sqlstate_55000");
 await rebindIngestGeneration(f.client,{from:A,to:B});
 await f.ok(f.worker(B));
});

test("zero-503 window: a Worker at A keeps admitting while the DB moves to B; {B,A} admits throughout; after the rebind B admits and A is refused",async()=>{
 const f=await fixture();
 const old=f.worker(A),both=f.worker(B,[B,A]),next=f.worker(B);
 // Worker PUT first: {B,A} against the untouched A database falls back to A.
 await f.ok(both);await f.ok(old);
 const w=await openGenerationOverlap(f.client,{from:A,to:B,ttlSeconds:3600,admitDuringApply:true,approvalSha256:APPROVAL});
 expect(w).toMatchObject({from:A,to:B,admitDuringApply:true});
 // Apply in progress: marker pending for B.
 await f.marker(B,"pending");
 await f.ok(old);await f.ok(both);
 expect((await f.refused(next)).code).toBe("ledger_sqlstate_55000"); // B is never admitted unverified
 // Phase 2 passed for B; policy still at A until the rebind.
 await f.marker(B,"pass");
 await f.ok(old);await f.ok(both);
 expect((await f.refused(next)).code).toBe("ledger_sqlstate_55000"); // a single-B Worker waits for the rebind
 const dry=await rebindIngestGeneration(f.client,{from:A,to:B,dryRun:true});
 expect(dry).toMatchObject({policy:"rebound",overlapClosed:true,dryRun:true});
 expect(await generationOverlapStatus(f.client)).toMatchObject({from:A,to:B}); // dry run rolled the close back
 const real=await rebindIngestGeneration(f.client,{from:A,to:B});
 expect(real).toMatchObject({policy:"rebound",overlapClosed:true});
 expect(await generationOverlapStatus(f.client)).toBeNull();
 expect((await f.db.query("SELECT closed_reason FROM sessions.ingest_generation_overlap_windows")).rows).toEqual([{closed_reason:"rebind"}]);
 // After the window: A refused, B admits, {B,A} admits (now at B).
 expect((await f.refused(old)).code).toBe("ledger_sqlstate_55000");
 await f.ok(next);await f.ok(both);
 // The {B,A} Worker's admitted attempts are bound to the generation each claim was admitted with.
 const gens=(await f.db.query("SELECT DISTINCT schema_generation g FROM sessions.ingest_claim_attempts ORDER BY 1")).rows.map((r:any)=>r.g);
 expect(gens).toEqual([A,B].sort());
 expect(await rebindIngestGeneration(f.client,{from:A,to:B})).toMatchObject({policy:"already",overlapClosed:false});
});

test("admitDuringApply false: the old generation is admitted only once B has PASSED (never during the apply)",async()=>{
 const f=await fixture();
 await openGenerationOverlap(f.client,{from:A,to:B,ttlSeconds:600,admitDuringApply:false,approvalSha256:APPROVAL});
 await f.marker(B,"pending");
 expect((await f.refused(f.worker(A))).code).toBe("ledger_sqlstate_55000");
 await f.marker(B,"pass");
 await f.ok(f.worker(A));
});

test("the window only bridges from -> the marker's generation: another target or a third generation is refused",async()=>{
 const f=await fixture();
 await openGenerationOverlap(f.client,{from:A,to:B,ttlSeconds:600,admitDuringApply:true,approvalSha256:APPROVAL});
 await f.marker(C,"pass"); // readiness moved somewhere the window does not name
 expect((await f.refused(f.worker(A))).code).toBe("ledger_sqlstate_55000");
 await f.marker(B,"pass");
 await f.ok(f.worker(A));
 expect((await f.refused(f.worker(C))).code).toBe("ledger_sqlstate_55000");
 // A Worker offering {C,A} is admitted only through A (the DB decides; C itself never is).
 await f.ok(f.worker(C,[C,A]));
 expect((await f.db.query("SELECT count(*)::int n FROM sessions.ingest_claim_attempts WHERE schema_generation=$1",[C])).rows[0]).toEqual({n:0});
});

test("window close (owner) and expiry both refuse A again; open sweeps an expired window",async()=>{
 const f=await fixture();
 await openGenerationOverlap(f.client,{from:A,to:B,ttlSeconds:600,admitDuringApply:true,approvalSha256:APPROVAL});
 await f.marker(B,"pass");
 await f.ok(f.worker(A));
 expect(await closeGenerationOverlap(f.client,{from:A,to:B,reason:"owner"})).toMatchObject({closed:true,reason:"owner"});
 expect((await f.refused(f.worker(A))).code).toBe("ledger_sqlstate_55000");
 expect(await closeGenerationOverlap(f.client,{from:A,to:B,reason:"owner"})).toEqual({closed:false});
 // Expiry: a window whose expires_at has passed admits nothing (owner-inserted with a past interval).
 await f.db.query("INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,true,$3,'test',clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '1 second')",[A,B,APPROVAL]);
 expect((await f.refused(f.worker(A))).code).toBe("ledger_sqlstate_55000");
 expect(await generationOverlapStatus(f.client)).toMatchObject({expired:true});
 // A fresh open (readiness PASS for its from) closes the expired one as 'expired'.
 await f.marker(A,"pass");
 await openGenerationOverlap(f.client,{from:A,to:B,ttlSeconds:60,admitDuringApply:true,approvalSha256:APPROVAL});
 expect((await f.db.query("SELECT closed_reason FROM sessions.ingest_generation_overlap_windows ORDER BY window_id")).rows.map((r:any)=>r.closed_reason)).toEqual(["owner","expired",null]);
});

test("an expired window for another pair admits nothing and does not block a rebind",async()=>{
 const f=await fixture();
 await f.db.query("INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,true,$3,'test',clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '1 second')",[A,C,APPROVAL]);
 await f.marker(B,"pass");
 expect(await rebindIngestGeneration(f.client,{from:A,to:B})).toMatchObject({policy:"rebound",overlapClosed:false});
 await f.ok(f.worker(B));
});

test("no downgrade at the rebind (dogpatch P1 causal probe): A -> B, then marker back at A, rebind B -> A is refused and an A Worker stays refused; an expired A -> B window does not weaken it",async()=>{
 const f=await fixture();
 // An expired A -> B window (closed as 'expired', never 'rebind') must not matter.
 await f.db.query("INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,false,$3,'t',clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '1 second')",[A,B,APPROVAL]);
 await f.marker(B,"pass");
 expect(await rebindIngestGeneration(f.client,{from:A,to:B})).toMatchObject({policy:"rebound"});
 expect((await f.db.query("SELECT closed_reason FROM sessions.ingest_generation_overlap_windows")).rows).toEqual([{closed_reason:"expired"}]);
 expect((await f.db.query("SELECT generation,successor FROM sessions.ingest_generation_overlap_retired")).rows).toEqual([{generation:A,successor:B}]);
 // Readiness goes back to A (e.g. an older image is re-applied): the rebind back must refuse.
 await f.marker(A,"pass");
 expect(await err(rebindIngestGeneration(f.client,{from:B,to:A}))).toContain("target_generation_retired");
 // Even bypassing the rebind helper, the policy itself refuses a retired generation (DB-side).
 const pol=(await f.db.query("SELECT source_identity_sha256 s,control_epoch e,privacy_baseline_sha256 p,retention_contract_sha256 r,source_proof_sha256 q,valid_until v FROM sessions.ingest_lazy_policy")).rows[0] as any;
 expect(await err(f.owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation:A,sourceIdentitySha256:pol.s,controlEpoch:pol.e,privacyBaselineSha256:pol.p,retentionContractSha256:pol.r,sourceProofSha256:pol.q,validUntil:new Date(pol.v).toISOString(),issuer:"relayhistory-cloud"}))).toContain("generation already retired");
 expect(await err(f.db.query("UPDATE sessions.ingest_lazy_policy SET generation=$1",[A]))).toContain("generation already retired");
 expect((await f.db.query("SELECT generation FROM sessions.ingest_lazy_policy")).rows).toEqual([{generation:B}]);
 // The retired A Worker is refused although readiness says A.
 expect((await f.refused(f.worker(A))).code).toBe("ledger_sqlstate_55000");
 // A window can never name A again either.
 await f.marker(B,"pass");
 expect(await err(openGenerationOverlap(f.client,{from:B,to:A,ttlSeconds:60,admitDuringApply:false,approvalSha256:APPROVAL}))).toContain("generation already retired");
 // The retirement record is append-only.
 expect(await err(f.db.query("DELETE FROM sessions.ingest_generation_overlap_retired"))).toContain("append-only");
 expect(await err(f.db.query("UPDATE sessions.ingest_generation_overlap_retired SET successor=NULL"))).toContain("append-only");
});

test("open requires exactly one lazy custody policy at from (dogpatch P2): a missing policy refuses",async()=>{
 const f=await fixture();
 await f.db.query("DELETE FROM sessions.ingest_lazy_policy");
 expect(await err(openGenerationOverlap(f.client,{from:A,to:B,ttlSeconds:600,admitDuringApply:false,approvalSha256:APPROVAL}))).toContain("custody policy is not at from");
 expect((await f.db.query("SELECT count(*)::int n FROM sessions.ingest_generation_overlap_windows")).rows[0]).toEqual({n:0});
 // Deleting the policy retired its generation too: it can never be reinstated.
 expect((await f.db.query("SELECT generation,successor FROM sessions.ingest_generation_overlap_retired")).rows).toEqual([{generation:A,successor:null}]);
});

test("owner-only: the ingest role cannot open, close, read or edit a window; a granted non-owner is still refused 42501",async()=>{
 const f=await fixture();
 const ingest=f.as("bulls96_session_ingest");
 const p=JSON.stringify({from:A,to:B,ttlSeconds:600,admitDuringApply:true,approvalSha256:APPROVAL});
 expect(await err(ingest("SELECT sessions.ingest_generation_overlap_open($1::jsonb)",[p]))).toContain("permission denied");
 expect(await err(ingest("SELECT sessions.ingest_generation_overlap_close($1::jsonb)",[JSON.stringify({from:A,to:B,reason:"owner"})]))).toContain("permission denied");
 expect(await err(ingest("SELECT sessions.ingest_generation_overlap_admits($1,$2,'pass')",[A,B]))).toContain("permission denied");
 expect(await err(ingest("SELECT count(*) FROM sessions.ingest_generation_overlap_windows"))).toContain("permission denied");
 expect(await err(ingest("INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,true,$3,'x',now(),now()+interval '1 hour')",[A,B,APPROVAL]))).toContain("permission denied");
 // Even with EXECUTE granted, the function's own owner check refuses.
 await f.db.query("GRANT EXECUTE ON FUNCTION sessions.ingest_generation_overlap_open(jsonb) TO bulls96_session_ingest");
 expect(await code(ingest("SELECT sessions.ingest_generation_overlap_open($1::jsonb)",[p]))).toBe("42501");
 await f.db.query("REVOKE EXECUTE ON FUNCTION sessions.ingest_generation_overlap_open(jsonb) FROM bulls96_session_ingest");
 // Owner/operator paths never use the window: the owner calling the fence at A during a window is refused.
 await openGenerationOverlap(f.client,{from:A,to:B,ttlSeconds:600,admitDuringApply:true,approvalSha256:APPROVAL});
 await f.marker(B,"pass");
 expect(await code(f.db.query("SELECT sessions.ingest_require_generation($1)",[A]))).toBe("55000");
});

test("no wildcard, no downgrade, explicit inputs, append-only record",async()=>{
 const f=await fixture();
 const raw=(p:unknown)=>f.db.query("SELECT sessions.ingest_generation_overlap_open($1::jsonb)",[JSON.stringify(p)]);
 const base={from:A,to:B,ttlSeconds:600,admitDuringApply:true,approvalSha256:APPROVAL};
 for(const bad of [{...base,from:"*"},{...base,to:"%"},{...base,to:A},{...base,ttlSeconds:59},{...base,ttlSeconds:14401},{...base,ttlSeconds:"600"},
  {...base,admitDuringApply:"yes"},{...base,approvalSha256:"x"},{...base,extra:1},{from:A,to:B}])
  expect(await code(raw(bad))).toBe("22023");
 // Readiness must be PASS for from, and the policy at from.
 expect(await err(raw({...base,from:C,to:B}))).toContain("readiness is not PASS for from");
 await f.marker(A,"pending");
 expect(await err(raw(base))).toContain("readiness is not PASS for from");
 await f.marker(A,"pass");
 await raw(base);
 expect(await err(raw({...base,to:C}))).toContain("a window is already open");
 // Append-only.
 expect(await err(f.db.query("UPDATE sessions.ingest_generation_overlap_windows SET expires_at=expires_at+interval '1 hour'"))).toContain("append-only");
 expect(await err(f.db.query("UPDATE sessions.ingest_generation_overlap_windows SET to_generation=$1",[C]))).toContain("append-only");
 expect(await err(f.db.query("DELETE FROM sessions.ingest_generation_overlap_windows"))).toContain("append-only");
 // At most 4 hours even for a direct owner insert.
 expect(await code(f.db.query("INSERT INTO sessions.ingest_generation_overlap_windows(from_generation,to_generation,admit_during_apply,approval_sha256,opened_by,opened_at,expires_at) VALUES($1,$2,true,$3,'x',now(),now()+interval '5 hours')",[A,C,APPROVAL]))).toBe("23514");
 // Rebind with a mismatched live window refuses.
 await f.marker(C,"pass");
 expect(await err(rebindIngestGeneration(f.client,{from:A,to:C}))).toContain("overlap_window_mismatch");
 // Complete A -> B, then try to go back: B -> A is a downgrade (A was retired).
 await f.marker(B,"pass");
 await rebindIngestGeneration(f.client,{from:A,to:B});
 expect(await err(raw({...base,from:B,to:A}))).toContain("generation already retired");
 // A forward window B -> C is fine.
 await raw({...base,from:B,to:C});
});

test("Worker config: INGEST_ACCEPT_GENERATIONS is 1-2 explicit hex64 values including the expected one, buffered only",async()=>{
 const {acceptedGenerations}=await import("../worker.ts");
 expect(acceptedGenerations({INGEST_EXPECT_GENERATION:A},"buffered")).toBeUndefined();
 expect(acceptedGenerations({INGEST_EXPECT_GENERATION:A,INGEST_ACCEPT_GENERATIONS:`${A},${B}`},"buffered")).toEqual([A,B]);
 expect(acceptedGenerations({INGEST_EXPECT_GENERATION:A,INGEST_ACCEPT_GENERATIONS:`${B},${A}`},"buffered")).toEqual([B,A]);
 for(const bad of ["*",`${A},*`,`${A},${B},${C}`,`${B},${C}`,`${A},${A}`,`${A}, ${B}`,`${A},`,A.toUpperCase(),`${A};${B}`])
  expect(()=>acceptedGenerations({INGEST_EXPECT_GENERATION:A,INGEST_ACCEPT_GENERATIONS:bad},"buffered")).toThrow();
 expect(()=>acceptedGenerations({INGEST_EXPECT_GENERATION:A,INGEST_ACCEPT_GENERATIONS:A},"canonical")).toThrow();
 const run=async()=>({rows:[]});
 expect(()=>sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:A,acceptGenerations:["*"]})).toThrow();
 expect(()=>sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:A,acceptGenerations:[B,C]})).toThrow();
 expect(()=>sqlLedger(run,{acceptanceMode:"canonical",acceptGenerations:[A]})).toThrow();
});
