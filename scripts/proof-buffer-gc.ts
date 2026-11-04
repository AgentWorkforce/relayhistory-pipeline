/** Synthetic only: real PG17 lock/lease/rollback controls, no production URLs. */
import pg from "pg";
import {execFileSync} from "node:child_process";
import {readFileSync,readdirSync,mkdirSync,writeFileSync} from "node:fs";
import {join} from "node:path";
import {createHash,randomUUID} from "node:crypto";
const out=process.env.BUFFER_GC_PROOF_OUT??"";
if(process.env.BUFFER_GC_DISPOSABLE!=="1"||!out.startsWith("/home/khaliqgant/relay-briefs/buffer-gc-real-"))throw new Error("disposable_only");
const info=JSON.parse(execFileSync("docker",["inspect","ingest-seed-proof-pg17-20261104"],{encoding:"utf8"}))[0];
if(info.Config.Labels["com.96bulls.proof"]!=="ingest-seed")throw new Error("container_owner");
const map=info.NetworkSettings.Ports["5432/tcp"][0];
if(map.HostIp!=="127.0.0.1")throw new Error("localhost_only");
const head=execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim();
const dirty=!!execFileSync("git",["status","--porcelain"],{encoding:"utf8"}).trim();
if(dirty&&!process.argv.includes("--allow-wip"))throw new Error("source_dirty");
mkdirSync(out,{recursive:false,mode:0o700});
const config=(database:string,name:string)=>({host:"127.0.0.1",port:Number(map.HostPort),user:"postgres",database,application_name:"buffer-gc-proof-"+name,connectionTimeoutMillis:5000,options:"-c statement_timeout=10000 -c lock_timeout=5000"});
const admin=new pg.Client(config("postgres","admin"));await admin.connect();
await admin.query("DROP DATABASE IF EXISTS ingest_buffer_gc_fixture WITH (FORCE)");
await admin.query("CREATE DATABASE ingest_buffer_gc_fixture");await admin.end();
const db=new pg.Client(config("ingest_buffer_gc_fixture","owner")),actor=new pg.Client(config("ingest_buffer_gc_fixture","actor")),blocker=new pg.Client(config("ingest_buffer_gc_fixture","blocker")),monitor=new pg.Client(config("ingest_buffer_gc_fixture","monitor"));
const clients=[db,actor,blocker,monitor];for(const c of clients){c.on("error",()=>{});await c.connect()}
const root=process.cwd(),proof:any={head,dirty,node:process.version,cases:[],failed:0};let stage="setup";
const assert=(v:unknown,why:string)=>{if(!v)throw new Error(why)};
const sha=(s:string)=>createHash("sha256").update(s).digest("hex");
const sleep=(ms:number)=>new Promise(r=>setTimeout(r,ms));
async function rejected(p:Promise<unknown>,code:string){const e=await p.then(()=>null,e=>e);assert(e?.code===code,"expected_"+code)}
async function check(name:string,work:()=>Promise<void>){stage=name;const t=Date.now();await work();proof.cases.push({name,ms:Date.now()-t,pass:true})}
async function rpc(name:string,p:unknown){await actor.query("BEGIN");try{await actor.query("SET LOCAL ROLE bulls96_session_ingest");const r=(await actor.query("SELECT sessions.ingest_buffer_gc_"+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0].r;await actor.query("COMMIT");return r}catch(e){await actor.query("ROLLBACK");throw e}}
let serial=0;
async function fixture(){
 const batch="batch-"+(++serial),claim=randomUUID(),token=randomUUID(),digest=sha(batch),body=sha(token),org="org-"+serial;
 const key=(hash:string)=>"96bulls/"+org+"/buffered-batches/v1/ws=ws/m=fixture/b="+sha(digest).slice(0,32)+"-"+hash.slice(0,16)+"-"+sha(claim).slice(0,32)+".staged.ndjson.zst";
 await db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at,publication_state) VALUES($1,'ws','origin',$2,'user',$3,'rhr_'||repeat('a',64),'committed',clock_timestamp(),'buffered')",[org,batch,digest]);
 await db.query("INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch) VALUES($1,$2,'ws','user','origin',$3,$4,$5,$6,0)",[claim,org,batch,digest,JSON.stringify([{source:"claude",session_id:"gone",epoch:0,reset_epoch:0},{source:"claude",session_id:"live",epoch:0,reset_epoch:0}]),JSON.stringify([{ord:0,source:"claude",session_id:"gone"},{ord:1,source:"claude",session_id:"live"}])]);
 await db.query("INSERT INTO sessions.ingest_stage_intents(stage_token,claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,stage_key,stage_sha256,stage_bytes,group_ordinals,reset_epoch,receipt_state,expires_at) VALUES($1,$2,$3,'ws','user','origin',$4,$5,$6,$7,10,'[[0],[1]]',0,'acknowledged',clock_timestamp()+interval '5 minutes')",[token,claim,org,batch,digest,key(body),body]);
 await db.query("INSERT INTO sessions.ingest_buffer_outbox(org_id,workspace_id,origin_id,batch_id,user_id,digest,active_stage_token) VALUES($1,'ws','origin',$2,'user',$3,$4)",[org,batch,digest,token]);
 await db.query("INSERT INTO sessions.ingest_fence_versions VALUES($1,'ws','user','origin','claude','gone',1)",[org]);
 const tickets=(await rpc("claim",{limit:100})).tickets;
 const ticket=tickets.find((x:any)=>x.stageToken===token);assert(ticket,"ticket");
 const hash=sha("child-"+token),spec={stageToken:token,leaseToken:ticket.leaseToken,key:key(hash),sha256:hash,bytes:8,groupOrdinals:[[1]]};
 return {org,batch,token,claim,ticket,spec};
}
async function waitLock(){for(let i=0;i<100;i++){if((await monitor.query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='buffer-gc-proof-actor' AND wait_event_type='Lock') AS yes")).rows[0].yes)return;await sleep(10)}throw new Error("no_actual_blocker")}
try{
 await db.query(readFileSync(join(root,"supabase/tests/supabase_shim.sql"),"utf8"));
 for(const name of ["session_directory_search_worker","session_token_verifier","session_ingest_roles"])await db.query(readFileSync(join(root,"supabase/bootstrap/"+name+".sql"),"utf8"));
 for(const name of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.query(readFileSync(join(root,"supabase/migrations/"+name),"utf8"));
 await check("prepare expiry after actual outbox lock rolls back child registration",async()=>{
  const f=await fixture();
  await db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()+interval '250 milliseconds' WHERE stage_token=$1",[f.token]);
  await blocker.query("BEGIN");await blocker.query("UPDATE sessions.ingest_buffer_outbox SET row_version=row_version WHERE org_id=$1",[f.org]);
  const attempt=rpc("prepare",f.spec).then(()=>null,e=>e);await waitLock();await sleep(350);await blocker.query("COMMIT");
  assert((await attempt)?.code==="40001","final_prepare_expiry");
  assert((await db.query("SELECT count(*)::int AS n FROM sessions.ingest_stage_intents WHERE parent_stage_token=$1",[f.token])).rows[0].n===0,"no_late_child");
  assert((await db.query("SELECT gc_replacement_stage_token FROM sessions.ingest_stage_intents WHERE stage_token=$1",[f.token])).rows[0].gc_replacement_stage_token===null,"no_late_parent_update");
 });
 await check("swap expiry after actual child lock rolls back pointer and ACK",async()=>{
  const f=await fixture(),child=await rpc("prepare",f.spec);
  await db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()+interval '250 milliseconds' WHERE stage_token=$1",[f.token]);
  await blocker.query("BEGIN");await blocker.query("UPDATE sessions.ingest_stage_intents SET stage_bytes=stage_bytes WHERE stage_token=$1",[child.replacementStageToken]);
  const attempt=rpc("swap",{stageToken:f.token,leaseToken:f.ticket.leaseToken,replacementStageToken:child.replacementStageToken}).then(()=>null,e=>e);await waitLock();await sleep(350);await blocker.query("COMMIT");
  assert((await attempt)?.code==="40001","final_swap_expiry");
  assert((await db.query("SELECT active_stage_token FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.org])).rows[0].active_stage_token===f.token,"old_pointer_retained");
  assert((await db.query("SELECT receipt_state FROM sessions.ingest_stage_intents WHERE stage_token=$1",[child.replacementStageToken])).rows[0].receipt_state==="preparing","no_late_ack");
 });
 await check("positive mixed swap and retired cleanup do not need original raw",async()=>{
  const f=await fixture(),child=await rpc("prepare",f.spec);
  assert((await rpc("swap",{stageToken:f.token,leaseToken:f.ticket.leaseToken,replacementStageToken:child.replacementStageToken})).swapped,"swap");
  assert((await rpc("finish",{stageToken:f.token,leaseToken:f.ticket.leaseToken,barrierVersionId:"verified-barrier-version"})).finished,"finish");
  assert((await db.query("SELECT active_stage_token FROM sessions.ingest_buffer_outbox WHERE org_id=$1",[f.org])).rows[0].active_stage_token===child.replacementStageToken,"kept_current");
 });
 await check("finish final expiry rolls back retirement acknowledgement after write delay",async()=>{
  const f=await fixture(),child=await rpc("prepare",f.spec);await rpc("swap",{stageToken:f.token,leaseToken:f.ticket.leaseToken,replacementStageToken:child.replacementStageToken});
  await db.query("CREATE FUNCTION sessions.gc_proof_delay() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.cleanup_state='redacted' THEN PERFORM pg_sleep(0.35);END IF;RETURN NEW;END$$;CREATE TRIGGER gc_proof_delay BEFORE UPDATE ON sessions.ingest_stage_intents FOR EACH ROW EXECUTE FUNCTION sessions.gc_proof_delay()");
  await db.query("UPDATE sessions.ingest_stage_intents SET gc_lease_until=clock_timestamp()+interval '200 milliseconds' WHERE stage_token=$1",[f.token]);
  await rejected(rpc("finish",{stageToken:f.token,leaseToken:f.ticket.leaseToken,barrierVersionId:"verified-barrier-version"}),"40001");
  assert((await db.query("SELECT cleanup_state FROM sessions.ingest_stage_intents WHERE stage_token=$1",[f.token])).rows[0].cleanup_state==="retired","finish_rollback");
  await db.query("DROP TRIGGER gc_proof_delay ON sessions.ingest_stage_intents;DROP FUNCTION sessions.gc_proof_delay()");
 });
 await sleep(100);
 assert((await monitor.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name='buffer-gc-proof-actor' AND state<>'idle'")).rows[0].n===0,"no_late_active_query");
 assert((await blocker.query("SELECT 1 AS n")).rows[0].n===1,"unrelated_client_preserved");
 proof.passed=proof.cases.length;proof.noLateQuery=true;
}catch(e){proof.failed=1;proof.error={stage,code:(e as any)?.code??(e as any)?.message};process.exitCode=1}
finally{for(const c of clients)await c.end().catch(()=>{});writeFileSync(join(out,"proof.json"),JSON.stringify(proof,null,2)+"\n");console.log(JSON.stringify(proof))}
