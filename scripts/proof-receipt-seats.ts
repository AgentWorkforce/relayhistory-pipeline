/** Actual owned Node24 -> full PG17 migrator -> buffered producer/MinIO -> ACK seats.
 * Source floor uses immutable684 exporter + exact9f full legacy schema, no prod auth.
 */
import pg from "pg";
import {execFileSync} from "node:child_process";
import {createHash} from "node:crypto";
import {readFileSync,mkdirSync,writeFileSync} from "node:fs";
import {runMigrations,generation} from "../src/hoopdb/migrate.ts";
import {SemanticPostgresExecutor} from "../src/search-features/semantic-copy-postgres.ts";
import {sqlLedger} from "../src/session-ingest/ledger-sql.ts";
import {orderedFixtureLedger} from "../src/session-ingest/__tests__/ordered-support.ts";
import {deliverBatch} from "../src/session-ingest/deliver.ts";
import {createS3Store,objectKey,type ObjectStore} from "../src/shared/vendor.ts";
import {createS3Eraser} from "../src/migration/neon-s3-erasure/eraser.ts";
import {sqlBufferedGcLedger,collectBufferedTicket} from "../src/session-ingest/buffered-gc.ts";
import {deliveryAccount} from "../src/session-ingest/vendor/delivery/prepare.ts";
import {DELIVERY_MAPPING_VERSION,type HistoryExportRecord,type HistoryExportBatch} from "../src/session-ingest/vendor/delivery/delivery-contracts.ts";
const out=process.env.RECEIPT_SEAT_PROOF_OUT??"";
if(process.env.RECEIPT_SEAT_DISPOSABLE!=="1"||!out.startsWith("/home/khaliqgant/relay-briefs/receipt-seats-proof-"))throw Error("owned_only");
function port(name:string,label:string,key:string){
 const d=JSON.parse(execFileSync("docker",["inspect",name],{encoding:"utf8"}))[0];
 if(d.Config.Labels["com.96bulls.proof"]!==label)throw Error("container_owner");
 const p=d.NetworkSettings.Ports[key][0];if(p.HostIp!=="127.0.0.1")throw Error("loopback_only");return Number(p.HostPort);
}
const targetPort=port("receipt-seats-pg17-20261104","receipt-seats-runtime","5432/tcp"),
 sourcePort=port("ordered-neon-origin-pg17-20261104","ordered-neon-origin","5432/tcp"),
 s3Port=port("buffer-gc-runtime-minio-20261104","buffer-gc-runtime","9000/tcp");
const head=execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim(),dirty=!!execFileSync("git",["status","--porcelain"],{encoding:"utf8"}).trim();
if(dirty&&!process.argv.includes("--allow-wip"))throw Error("source_dirty");
mkdirSync(out,{recursive:false,mode:0o700});const proof:any={head,dirty,node:process.version,cases:[]};
const yes=(v:unknown,name:string)=>{if(!v)throw Error(name)};
const hash=(b:string|Uint8Array)=>createHash("sha256").update(b).digest("hex");
const cfg=(p:number,database:string,user="postgres")=>({host:"127.0.0.1",port:p,database,user,connectionTimeoutMillis:1000,application_name:"receipt-seat-proof"});
const credentials={accessKeyId:"gc-fixture-access",secretAccessKey:"gc-fixture-only-key-20261104"},endpoint="http://127.0.0.1:"+s3Port,bucket="receipt-seats-fixture";
const store=createS3Store({bucket,region:"us-east-1",endpoint,credentials});
const eraser=createS3Eraser({bucket,region:"us-east-1",endpoint,credentials});
const resetDb=async(p:number,name:string)=>{const a=new pg.Client(cfg(p,"postgres"));await a.connect();await a.query("DROP DATABASE IF EXISTS "+name+" WITH(FORCE)");await a.query("CREATE DATABASE "+name);await a.end()};
await resetDb(targetPort,"receipt_seat_fixture");await resetDb(sourcePort,"receipt_floor_source_fixture");
const owner=new pg.Client(cfg(targetPort,"receipt_seat_fixture")),sourceOwner=new pg.Client(cfg(sourcePort,"receipt_floor_source_fixture"));
owner.on("error",()=>{});sourceOwner.on("error",()=>{});await owner.connect();await sourceOwner.connect();
let pool:pg.Pool|undefined,stage="setup",serial=0;
async function check(name:string,f:()=>Promise<void>){stage=name;const at=Date.now();await f();proof.cases.push({name,pass:true,ms:Date.now()-at})}
async function refuses(p:Promise<unknown>){const e=await p.then(()=>null,e=>e);yes(e,"expected_refusal");return e}
try{
 await runMigrations(owner,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}});
 const expected=await generation();proof.generation=expected;
 await owner.query(readFileSync(new URL("../supabase/bootstrap/session_ingest_role.sql",import.meta.url),"utf8").split("\n").filter(x=>!x.startsWith("\\")).join("\n"));
 pool=new pg.Pool({...cfg(targetPort,"receipt_seat_fixture","bulls96_session_ingest"),max:4});pool.on("error",()=>{});
 const x=new SemanticPostgresExecutor(pool),run=(s:string,p:unknown[])=>x.queryBounded<Record<string,unknown>>(s,p,{signal:AbortSignal.timeout(5000),deadline:Date.now()+5000});
 const base=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:expected}),ledger=orderedFixtureLedger(base,owner);
 // Synthetic object service, versioned: real adapters/conditional PUT/GC are used.
 const {signRequest}=await import("../packages/object-store/src/sigv4.ts");
 async function s3(method:string,query="",body=""){const url=new URL(endpoint+"/"+bucket+query),headers=new Headers({"content-type":"application/xml"});await signRequest({method,url,headers,payloadSha256:hash(body),region:"us-east-1",credentials});const r=await fetch(url,{method,headers,...(body?{body}:{}),signal:AbortSignal.timeout(5000)});if(!r.ok)throw Error("fixture_s3_"+r.status);return r}
 await s3("HEAD").catch(async e=>{if(e.message!=="fixture_s3_404")throw e;await s3("PUT")});
 await s3("PUT","?versioning",'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>');
 const artifactPath=process.env.RECEIPT_SEAT_SOURCE_FIXTURE??"";
 yes(/^\/home\/khaliqgant\/relay-briefs\/ordered-control-neon-source-[A-Za-z0-9_-]+\/fixture\.json$/.test(artifactPath),"owned_source_fixture_path");
 const artifact=JSON.parse(readFileSync(artifactPath,"utf8"));
 yes(artifact.sourceHead==="9f57617874a056b7a82d2324cb3c024b8d9e04be"&&hash(artifact.source0077)===artifact.source0077Sha256,"source_schema_pin");
 await sourceOwner.query("DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='ordered_neon_runtime') THEN CREATE ROLE ordered_neon_runtime NOLOGIN NOINHERIT NOSUPERUSER;END IF;END$$");
 await sourceOwner.query("BEGIN;CREATE TABLE public.auth_sessions(id text PRIMARY KEY);"+artifact.baselineStatements.join(";")+";UPDATE sessions.upload_cleanup_rollout SET phase=5;COMMIT");
 await sourceOwner.query("BEGIN;"+artifact.source0077+";"+artifact.finalGrantStatement+";COMMIT");
 // Real sequence/cache allocations before the owned source cutoff. No row max
 // is used. Projection deliberately exceeds catalog and JS safe integer range.
 await sourceOwner.query("ALTER SEQUENCE sessions.delivery_catalog_acceptance_seq CACHE 8;ALTER SEQUENCE sessions.delivery_projection_acceptance_seq CACHE 8");
 await sourceOwner.query("SELECT setval('sessions.delivery_catalog_acceptance_seq',9007199254740993,true),setval('sessions.delivery_projection_acceptance_seq',9007199254741093,true)");
 const allocated=(await sourceOwner.query("SELECT nextval('sessions.delivery_catalog_acceptance_seq')::text AS catalog,nextval('sessions.delivery_projection_acceptance_seq')::text AS projection")).rows[0];
 const epoch="3".repeat(64);await sourceOwner.query("SELECT sessions.owner_delivery_cutover_v1('seeding',$1)",[epoch]);
 await sourceOwner.query("DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='receipt_floor_reader') THEN CREATE ROLE receipt_floor_reader NOLOGIN NOINHERIT NOSUPERUSER;END IF;END$$;GRANT USAGE ON SCHEMA sessions TO receipt_floor_reader;GRANT SELECT ON sessions.delivery_records,sessions.uploaded_support_identities,sessions.deleted_upload_sessions,sessions.session_resets TO receipt_floor_reader;GRANT SELECT ON SEQUENCE sessions.delivery_catalog_acceptance_seq,sessions.delivery_projection_acceptance_seq TO receipt_floor_reader;GRANT EXECUTE ON FUNCTION sessions.delivery_cutover_status() TO receipt_floor_reader");
 const exporterPath=process.env.RECEIPT_SEAT_FLOOR_EXPORTER??"";
 yes(/^\/home\/khaliqgant\/relay-briefs\/receipt-floor-exporter-[A-Za-z0-9_-]+\/delivery-cutover-baseline\.mjs$/.test(exporterPath),"owned_floor_exporter_path");
 yes(hash(readFileSync(exporterPath))==="cd50cd71090c17407a26e50ea3108623784bedf35adf0e725a63a948fe4f5860","source_exporter_pin");
 const {exportPrivacyBaseline}=await import(exporterPath);
 const source={async readOnly<T>(f:(db:any)=>Promise<T>){const c=new pg.Client(cfg(sourcePort,"receipt_floor_source_fixture"));await c.connect();try{await c.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;SET LOCAL ROLE receipt_floor_reader");const r=await f({query:(s:string,p?:unknown[])=>c.query(s,p)});await c.query("COMMIT");return r}catch(e){await c.query("ROLLBACK");throw e}finally{await c.end()}}};
 const descriptor=await exportPrivacyBaseline(source,epoch,async()=>{}, {page:500,captureAcceptanceFloor:true});
 const floor=descriptor.baselineAcceptanceFloor;yes(descriptor.complete===true&&BigInt(floor.allocatedHighWater)>BigInt(allocated.projection),"allocated_cache_floor_not_nextval_or_rowmax");
 const sourceIdentity=hash(JSON.stringify((await sourceOwner.query("SELECT current_database() AS database,oid::text,current_setting('server_version_num') AS version FROM pg_database WHERE datname=current_database()")).rows[0]));
 const baseline={...floor,generation:expected,sourceIdentitySha256:sourceIdentity,sourceProofSha256:hash(JSON.stringify(descriptor))};
 proof.sourceFloor={exporterHead:"684ddb1b424075d7b3b1a5a3f0617a879cee91e7",sourceSchemaHead:artifact.sourceHead,descriptor,sourceIdentitySha256:sourceIdentity,sourceProofSha256:baseline.sourceProofSha256,emittedBeforeCutoff:allocated,ownedSeedingNoWriters:true};
 const initialize=()=>owner.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb) AS r",[JSON.stringify(baseline)]);
 const last=async()=>(await owner.query("SELECT last_seat::text AS s FROM sessions.ingest_receipt_seat_clock")).rows[0]?.s as string|undefined;
 async function fixture(){
  const n=++serial,tenant={orgId:"seat-org-"+n,workspaceId:"ws-"+n,userId:"user-"+n},origin="origin-"+n;
  await owner.query("BEGIN;SET LOCAL ROLE bulls96_session_ingest_seeder");
  try{await owner.query("SELECT sessions.ingest_seed_fence_baseline($1::jsonb)",[JSON.stringify({org_id:tenant.orgId,workspace_id:tenant.workspaceId,user_id:tenant.userId,deletions:[],resets:[]})]);await owner.query("COMMIT")}catch(e){await owner.query("ROLLBACK");throw e}
  await owner.query("INSERT INTO sessions.ingest_buffer_admission VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',$3)",[tenant.orgId,expected,epoch]);
  const rec=(id:string,text:string,revision=7,o=origin,session:string|null="session"):HistoryExportRecord=>({schema_version:1,origin_id:o,revision_id:id+":"+revision+":"+text.slice(0,8),record_id:id,revision,kind:session?"session_event":"presence",source:"claude",session_id:session,operation:"upsert",payload:{text,ts_ms:1761955200000}});
  const batch=async(id:string,records:HistoryExportRecord[],o=origin):Promise<HistoryExportBatch>=>({schema_version:1,origin_id:o,batch_id:id,job_id:"job-"+id,generation:1,destination_id:"fixture",instance_id:"fixture",account_id:await deliveryAccount(tenant),mapping_version:DELIVERY_MAPPING_VERSION,records});
  const deliver=async(id:string,records:HistoryExportRecord[],o=origin,l=ledger)=>deliverBatch({ledger:l,acceptanceMode:"buffered",target:{store,keyRoot:"96bulls",sleep:async()=>{}}},tenant,await batch(id,records,o));
  const stage=async(id:string)=>(await owner.query("SELECT i.* FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token WHERE o.org_id=$1 AND o.batch_id=$2",[tenant.orgId,id])).rows[0];
  const seats=async(id:string)=>(await owner.query("SELECT sessions.ingest_receipt_seats_for_stage($1::uuid) AS r",[(await stage(id)).stage_token])).rows[0].r;
  return{tenant,origin,rec,batch,deliver,stage,seats};
 }
 await check("real source two cached allocators plus readonly684 export; no baseline means no staged or suppressed ACK",async()=>{
  const f=await fixture();await refuses(f.deliver("cold",[f.rec("cold","not acknowledged")]));
  await ledger.setSessionUploads({tenant:f.tenant,originId:f.origin,sessions:[["claude","session"]],enabled:false});
  await refuses(f.deliver("suppressed",[f.rec("suppressed","still no ACK")]));
  yes((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===0,"no_uninitialized_outbox");
  await refuses(owner.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify({...baseline,allocatedHighWater:floor.sourceSequences[0].allocatedHighWater})]));
  yes(await last()===undefined,"invalid_lower_floor_not_initialized");
  await refuses(owner.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify({...baseline,sourceSequences:[...floor.sourceSequences,floor.sourceSequences[0]]})]));
  await initialize();yes(await last()===floor.allocatedHighWater,"init_exact_allocated_MAX");
 });
 await check("actual producer/S3 first ACK seats original ordinals above both allocators and replay never reseats",async()=>{
  const f=await fixture(),records=[f.rec("one","first"),f.rec("two","second")];
  yes((await f.deliver("initial",records)).outcome==="accepted_buffered","real_ack");
  const s=await f.seats("initial");yes(s.records.map((r:any)=>r.ord).join(",")==="0,1","original_ordinals");
  yes(s.records[0].seat===(BigInt(floor.allocatedHighWater)+1n).toString()&&s.records[1].seat===(BigInt(floor.allocatedHighWater)+2n).toString(),"above_cached_source_floor");
  const before=await last();yes((await f.deliver("initial",records)).outcome==="replay"&&await last()===before,"replay_no_reseat");
  yes(s.records.every((r:any)=>r.candidateDigest===r.firstDigest)&&s.lastAssignedSeat===before,"exact_first_body_and_upper_bound");
 });
 await check("production500-row ACK and exact500 original-ordinal maps stay inside existing role deadline",async()=>{
  const f=await fixture(),before=await last();
  const records=Array.from({length:500},(_,i)=>f.rec("record-"+i+"-"+"r".repeat(450),"synthetic payload",7,f.origin,"session-"+i+"-"+"s".repeat(450)));
  const body=await f.batch("capacity500",records);yes(Buffer.byteLength(JSON.stringify({protocolVersion:1,batch:body}))<2097152,"valid_wire_body_500");
  const at=Date.now();yes((await deliverBatch({ledger,acceptanceMode:"buffered",target:{store,keyRoot:"96bulls",sleep:async()=>{}}},f.tenant,body)).outcome==="accepted_buffered","capacity500_ACK");
  const map=await f.seats("capacity500");yes(map.records.length===500&&map.records.every((r:any,i:number)=>r.ord===i&&r.seat===(BigInt(before!)+BigInt(i)+1n).toString()),"capacity500_exact_ord_and_seat");
  proof.capacity500={ms:Date.now()-at,wireBytes:Buffer.byteLength(JSON.stringify({protocolVersion:1,batch:body})),records:500,existingRoleStatementTimeoutMs:5000};
 });
 await check("later same-origin same revision keeps first-body seat; delayed first PUT loses to earlier durable ACK",async()=>{
  const f=await fixture();let release!:()=>void,waiting!:()=>void;
  const gate=new Promise<void>(r=>release=r),entered=new Promise<void>(r=>waiting=r);
  const delayed={...ledger,async commitBuffered(p:any){waiting();await gate;return base.commitBuffered!(p)}};
  const a=f.deliver("delayed",[f.rec("shared","delayed body")],f.origin,delayed);await entered;
  await f.deliver("first-ack",[f.rec("shared","first durable body")]);const b=(await f.seats("first-ack")).records[0];
  release();yes((await a).outcome==="accepted_buffered","later_ack_kept_receipt");
  const late=(await f.seats("delayed")).records[0];yes(late.seat===b.seat&&late.firstDigest===b.firstDigest&&late.candidateDigest!==late.firstDigest,"immutable_first_ACK_body");
 });
 await check("equal revision cross-source/session aliases preserve original ACK decision after winner erase; new ACK gets new lifecycle",async()=>{
  const f=await fixture(),a=f.rec("alias","first A",7,f.origin,"session-A"),
   b={...f.rec("alias","later B",7,f.origin,"session-B"),source:"codex"};
  await f.deliver("alias-A",[a]);await f.deliver("alias-B",[b]);
  const first=(await f.seats("alias-A")).records[0],loser=(await f.seats("alias-B")).records[0];
  yes(first.bodyAccepted===true&&loser.bodyAccepted===false&&loser.seat===first.seat
   &&loser.firstDigest===first.firstDigest&&loser.candidateDigest!==loser.firstDigest,"alias_first_ACK_not_scope_key");
  await ledger.setSessionUploads({tenant:f.tenant,originId:f.origin,sessions:[["claude","session-A"]],enabled:false});
  yes(JSON.stringify((await f.seats("alias-B")).records[0])===JSON.stringify(loser),"old_loser_not_promoted_after_erasure");
  await f.deliver("alias-B-new",[b]);const newer=(await f.seats("alias-B-new")).records[0];
  yes(newer.bodyAccepted===true&&newer.firstDigest===newer.candidateDigest&&BigInt(newer.seat)>BigInt(first.seat),"new_ACK_new_lifecycle");
  yes(JSON.stringify((await f.seats("alias-B")).records[0])===JSON.stringify(loser),"old_loser_survives_new_winner");
  yes((await f.deliver("alias-B",[b])).outcome==="replay"&&(await f.seats("alias-B")).records[0].bodyAccepted===false,"replay_loser_no_reseat");
 });
 await check("current higher-revision custody supersedes historical alias; erase allows fresh lower revision without promoting old stale ACK",async()=>{
  const f=await fixture();
  await f.deliver("move-A",[f.rec("move","A",1,f.origin,"A")]);await f.deliver("move-B",[f.rec("move","B",2,f.origin,"B")]);
  const a=(await f.seats("move-A")).records[0],b=(await f.seats("move-B")).records[0];
  await f.deliver("old-live",[f.rec("move","C",1,f.origin,"C")]);const stale=(await f.seats("old-live")).records[0];
  yes(stale.bodyAccepted===false&&stale.seat===b.seat,"lowerrev_current_custody_refused");
  await ledger.setSessionUploads({tenant:f.tenant,originId:f.origin,sessions:[["claude","B"]],enabled:false});
  await f.deliver("fresh-lower",[f.rec("move","A fresh",1,f.origin,"A")]);const fresh=(await f.seats("fresh-lower")).records[0];
  yes(fresh.bodyAccepted===true&&BigInt(fresh.seat)>BigInt(b.seat)&&fresh.seat!==a.seat,"erased_current_custody_new_lowerrev_lifecycle");
  yes(JSON.stringify((await f.seats("old-live")).records[0])===JSON.stringify(stale),"old_stale_ACK_never_promoted");
 });
 await check("cross-origin ACK order is authoritative regardless stage PUT order",async()=>{
  const f=await fixture(),other=f.origin+"-other";let release!:()=>void,waiting!:()=>void;
  const gate=new Promise<void>(r=>release=r),entered=new Promise<void>(r=>waiting=r);
  const delayed={...ledger,async commitBuffered(p:any){waiting();await gate;return base.commitBuffered!(p)}};
  const a=f.deliver("origin-A",[f.rec("same-public-id","A",7,f.origin)],f.origin,delayed);await entered;
  await f.deliver("origin-B",[f.rec("same-public-id","B",7,other)],other);const b=(await f.seats("origin-B")).records[0];
  release();await a;const late=(await f.seats("origin-A")).records[0];yes(BigInt(b.seat)<BigInt(late.seat),"first_ACK_order_not_PUT_or_drain_time");
 });
 await check("concurrent independent-user ACKs serialize one clock and both retain unique seats",async()=>{
  const a=await fixture(),b=await fixture(),lock=new pg.Client(cfg(targetPort,"receipt_seat_fixture"));await lock.connect();
  try{
   await lock.query("BEGIN;SELECT 1 FROM sessions.ingest_receipt_seat_clock FOR UPDATE");
   const first=a.deliver("parallel-A",[a.rec("parallel","A")]).then(value=>({value,error:null as unknown}),error=>({value:null,error}));
   const second=b.deliver("parallel-B",[b.rec("parallel","B")]).then(value=>({value,error:null as unknown}),error=>({value:null,error}));
   let waiting=0;for(let n=0;n<200;n++){waiting=(await owner.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name='receipt-seat-proof' AND wait_event_type='Lock' AND query LIKE '%ingest_commit_buffered_batch%'")).rows[0].n;if(waiting>=2)break;await new Promise(r=>setTimeout(r,5))}
   yes(waiting>=2,"two_real_ACKs_wait_clock");await lock.query("COMMIT");
   const both=await Promise.all([first,second]);yes(both.every(r=>r.value?.outcome==="accepted_buffered"&&!r.error),"both_serialized_ACKs");
   const sa=(await a.seats("parallel-A")).records[0].seat,sb=(await b.seats("parallel-B")).records[0].seat;
   yes(sa!==sb,"unique_global_clock_seats");
  }finally{await lock.query("ROLLBACK").catch(()=>{});await lock.end()}
 });
 await check("clock row wait crosses intent deadline: receipt/outbox/maps/clock all roll back",async()=>{
  const f=await fixture(),before=await last(),lock=new pg.Client(cfg(targetPort,"receipt_seat_fixture"));await lock.connect();
  try{
   const blocked={...ledger,async commitBuffered(p:any){
    await owner.query("UPDATE sessions.ingest_stage_intents SET expires_at=clock_timestamp()+interval '0.2 second' WHERE stage_token=$1",[p.stageToken]);
    await lock.query("BEGIN;SELECT 1 FROM sessions.ingest_receipt_seat_clock FOR UPDATE");
    const committing=base.commitBuffered!(p).then(()=>({error:null as unknown}),error=>({error}));
    let waiting=false;for(let n=0;n<100;n++){waiting=(await owner.query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='receipt-seat-proof' AND wait_event_type='Lock' AND query LIKE '%ingest_commit_buffered_batch%') AS b")).rows[0].b;if(waiting)break;await new Promise(r=>setTimeout(r,5))}
    yes(waiting,"actual_clock_row_wait");await new Promise(r=>setTimeout(r,250));await lock.query("COMMIT");const result=await committing;if(result.error)throw result.error;
   }};
   await refuses(f.deliver("expired-clock",[f.rec("clock","no ACK")],f.origin,blocked));
   yes(await last()===before,"clock_rollback");
   yes((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===0,"no_expired_receipt");
   yes((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_receipt_ord_seats m JOIN sessions.ingest_claim_attempts a USING(claim_token) WHERE a.org_id=$1",[f.tenant.orgId])).rows[0].n===0,"no_uncommitted_seats");
  }finally{await lock.query("ROLLBACK").catch(()=>{});await lock.end()}
 });
 await check("GC child inherits original claim/ordinal seats and never reseats surviving groups",async()=>{
  const f=await fixture();await f.deliver("mixed",[f.rec("gone","erase",7,f.origin,"gone"),f.rec("kept","keep",7,f.origin,"kept")]);
  const original=await f.seats("mixed"),before=await last();
  await ledger.setSessionUploads({tenant:f.tenant,originId:f.origin,sessions:[["claude","gone"]],enabled:false});
  const gc=sqlBufferedGcLedger({query:run},expected),tickets=await gc.claim(100),ticket=tickets.find(t=>t.key.startsWith("96bulls/"+f.tenant.orgId+"/"));yes(ticket,"gc_ticket");
  yes(await collectBufferedTicket(ticket!,{ledger:gc,store,eraser,keyRoot:"96bulls"})==="finished","real_GC_child");
  const after=await f.seats("mixed");yes(after.records.length===1&&after.records[0].ord===1&&after.records[0].seat===original.records[1].seat,"same_original_kept_seat");
  yes(await last()===before,"GC_no_reseat");
 });
 await check("COMMIT-time rollback loses no seat; retry mints exactly once, lost reply replays without reseating",async()=>{
  const f=await fixture(),before=await last(),records=[f.rec("commit","atomic commit")];
  await owner.query("CREATE FUNCTION public.receipt_seat_abort_commit() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'owned commit abort' USING ERRCODE='P0001';END$$;CREATE CONSTRAINT TRIGGER receipt_seat_abort_commit AFTER INSERT ON sessions.ingest_receipt_ord_seats DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.receipt_seat_abort_commit()");
  try{await refuses(f.deliver("commit-abort",records));yes(await last()===before,"commit_error_counter_rollback");
   yes((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===0,"commit_error_outbox_rollback");
  }finally{await owner.query("DROP TRIGGER receipt_seat_abort_commit ON sessions.ingest_receipt_ord_seats;DROP FUNCTION public.receipt_seat_abort_commit()")}
  await f.deliver("commit-abort",records);yes((await f.seats("commit-abort")).records[0].seat===(BigInt(before!)+1n).toString(),"retry_one_first_seat");
  const second=[f.rec("reply","committed before lost reply")],lost={...ledger,async commitBuffered(p:any){await base.commitBuffered!(p);throw Error("owned_reply_lost")}};
  await refuses(f.deliver("reply-lost",second,f.origin,lost));const at=await last();
  yes((await f.deliver("reply-lost",second)).outcome==="replay"&&await last()===at,"reply_retry_no_reseat");
 });
 await check("reset/scope epochs create a new key while equal revision within one epoch preserves first body",async()=>{
  const f=await fixture();await f.deliver("before-reset",[f.rec("reused","before")]);const old=(await f.seats("before-reset")).records[0];
  await ledger.resetUploadedSessions(f.tenant,"seat-reset");
  await ledger.setSessionUploads({tenant:f.tenant,originId:f.origin,sessions:[["claude","session"]],enabled:true});
  await f.deliver("after-reset",[f.rec("reused","after")]);const next=(await f.seats("after-reset")).records[0];
  yes(BigInt(next.seat)>BigInt(old.seat)&&next.resetEpoch==="1"&&next.fenceEpoch!=="0","new_scope_seat_after_reset");
 });
 await check("missing non-null-session original epoch refuses instead of synthesizing epoch zero",async()=>{
  const f=await fixture();await f.deliver("epoch",[f.rec("epoch","original snapshot")]);const t=await f.stage("epoch"),before=await last();
  await owner.query("BEGIN");
  try{
   await owner.query("UPDATE sessions.ingest_claim_attempts SET epochs='[]' WHERE claim_token=$1",[t.claim_token]);
   await refuses(owner.query("SELECT sessions.ingest_receipt_seats_for_stage($1::uuid)",[t.stage_token]));
  }finally{await owner.query("ROLLBACK")}
  yes(await last()===before,"missing_epoch_no_reseat");
 });
 await check("missing original map refuses materialization and never invents a replacement seat",async()=>{
  const f=await fixture();await f.deliver("map",[f.rec("mapping","original")]);const t=await f.stage("map"),before=await last();
  await owner.query("BEGIN");
  try{
   await owner.query("DELETE FROM sessions.ingest_receipt_ord_seats WHERE claim_token=$1",[t.claim_token]);
   await refuses(owner.query("SELECT sessions.ingest_receipt_seats_for_stage($1::uuid)",[t.stage_token]));
  }finally{await owner.query("ROLLBACK")}
  yes(await last()===before&&(await f.seats("map")).records.length===1,"no_missing_map_reseat");
 });
 await check("BIGINT counter exhaustion rolls back partial ordinal maps and ACK as one transaction",async()=>{
  const f=await fixture(),before=await last();
  await owner.query("UPDATE sessions.ingest_receipt_seat_clock SET last_seat=9223372036854775806");
  try{
   await refuses(f.deliver("overflow",[f.rec("max-one","first"),f.rec("max-two","second")]));
   yes(await last()==="9223372036854775806","no_partial_counter_advance");
   yes((await owner.query("SELECT count(*)::int AS n FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.tenant.orgId])).rows[0].n===0,"no_overflow_ACK");
  }finally{await owner.query("UPDATE sessions.ingest_receipt_seat_clock SET last_seat=$1",[before])}
 });
 await check("old genuine buffered ACK cannot be retrofitted during530 installation",async()=>{
  const oldPort=port("buffer-gc-runtime-pg17-20261104","buffer-gc-runtime","5432/tcp"),old=new pg.Client(cfg(oldPort,"buffer_gc_runtime_fixture"));await old.connect();
  try{
   yes((await old.query("SELECT count(*)::int AS n FROM sessions.ingest_batches WHERE publication_state='buffered' AND state='committed' AND buffer_receipt_kind='staged'")).rows[0].n>0,"known_actual_preseat_ACK");
   const sql=readFileSync(new URL("../supabase/migrations/20261104053000_ingest_first_ack_seats.sql",import.meta.url),"utf8");
   const guard=sql.match(/DO \$old_ack\$[\s\S]*?\$old_ack\$;/)?.[0];yes(guard,"exact_install_guard");
   await old.query("BEGIN READ ONLY");await refuses(old.query(guard!));
  }finally{await old.query("ROLLBACK").catch(()=>{});await old.end()}
 });
 await check("mixed apply creator refuses before new seat DDL without ownership transfer or grants",async()=>{
  await owner.query("DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='receipt_other_creator') THEN CREATE ROLE receipt_other_creator NOLOGIN NOINHERIT NOSUPERUSER;END IF;END$$;GRANT USAGE ON SCHEMA sessions TO receipt_other_creator");
  const sql=readFileSync(new URL("../supabase/migrations/20261104053000_ingest_first_ack_seats.sql",import.meta.url),"utf8");
  const guard=sql.match(/DO \$creator\$[\s\S]*?\$creator\$;/)?.[0];yes(guard,"exact_creator_guard");
  await owner.query("BEGIN;SET LOCAL ROLE receipt_other_creator");
  try{const e=await refuses(owner.query(guard!));yes(e.code==="55000","creator_coherence_verdict")}finally{await owner.query("ROLLBACK")}
 });
 await check("schema-generation revalidation cannot reseed an existing immutable clock baseline",async()=>{
  const before=await last(),next="e".repeat(64);
  await owner.query("BEGIN");
  try{
   await owner.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status='pass'",[next]);await owner.query("UPDATE sessions.ingest_buffer_admission SET schema_generation=$1",[next]);
   await owner.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify({...baseline,generation:next})]);
   yes(await last()===before,"schema_upgrade_no_reseed");
  }finally{await owner.query("ROLLBACK")}
 });
 await check("initializer is immutable/idempotent and runtime cannot call private seat/clock helpers",async()=>{
  const before=await last();await initialize();yes(await last()===before,"idempotent_init_no_clock_reset");
  await refuses(owner.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)",[JSON.stringify({...baseline,sourceIdentitySha256:"e".repeat(64)})]));
  for(const sql of ["SELECT * FROM sessions.ingest_receipt_seat_clock","SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)","SELECT sessions.ingest_receipt_seats_for_stage(NULL::uuid)"])
   await refuses(run(sql,sql.includes("$1")?[JSON.stringify(baseline)]:[]));
  yes(await last()===before,"refusal_no_counter_change");
 });
 proof.passed=proof.cases.length;proof.maxRSSBytes=process.resourceUsage().maxRSS*1024;
}catch(e){proof.error={stage,code:(e as any)?.code??(e as any)?.message};process.exitCode=1}
finally{await Promise.allSettled([owner.end(),sourceOwner.end(),...(pool?[pool.end()]:[])]);writeFileSync(out+"/proof.json",JSON.stringify(proof,null,2)+"\n");console.log(JSON.stringify(proof))}
