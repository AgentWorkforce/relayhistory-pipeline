/** Owned ONLY actual PG17 History inventory proof. No existing fixture reset. */
import pg from"pg";import assert from"node:assert/strict";import{execFileSync}from"node:child_process";import{readFileSync,writeFileSync}from"node:fs";
import{runMigrations,generation}from"../src/hoopdb/migrate.ts";
if(process.env.HISTORY_DISPOSABLE!=="1")throw Error("owned_fixture_required");
const container=JSON.parse(execFileSync("docker",["inspect","canonical-history-proof-pg17-20261104"],{encoding:"utf8"}))[0];assert.equal(container.Config.Labels["com.96bulls.proof"],"canonical-history");const host=container.NetworkSettings.Ports["5432/tcp"][0];assert.equal(host.HostIp,"127.0.0.1");
const database="history_inventory_"+Date.now(),cfg=(user="postgres")=>({host:"127.0.0.1",port:Number(host.HostPort),database,user,connectionTimeoutMillis:1000});
const admin=new pg.Client({...cfg(),database:"postgres"});await admin.connect();await admin.query("CREATE DATABASE "+database);await admin.end();
const owner=new pg.Client(cfg());await owner.connect();let pool:pg.Pool|undefined;
const proof:any={head:execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim(),dirty:execFileSync("git",["status","--porcelain"],{encoding:"utf8"}).trim(),database,node:process.version,cases:[]};let stage="migrate";
const check=async(name:string,f:()=>Promise<void>)=>{stage=name;const start=Date.now();await f();proof.cases.push({name,ms:Date.now()-start,pass:true});console.log("PASS "+name)};
try{
 await runMigrations(owner,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}});
 await owner.query(readFileSync("supabase/bootstrap/session_ingest_role.sql","utf8").split("\n").filter(s=>!s.startsWith("\\")).join("\n"));
 await owner.query(readFileSync("supabase/bootstrap/session_ingest_verify.sql","utf8"));
 const gen=await generation();proof.schemaGeneration=gen;pool=new pg.Pool({...cfg("bulls96_session_ingest"),max:3});
 const call=async(name:string,p:any)=>(await pool!.query("SELECT sessions.ingest_history_inventory_"+name+"($1::jsonb) AS result",[JSON.stringify({...p,generation:gen})])).rows[0].result;
 const row=(id:number)=>({org_id:id===200?"foreign-org":"history-org",workspace_id:"ws",user_id:id===200?"foreign-user":"user",harness:"claude",session_id:"alias"+id,machine_id:"srv_fixture",created_at:"2026-11-01T00:00:00Z",last_activity_at:"2026-11-01T00:00:00Z",storage_provider:"s3",bucket:"history-fixture",manifest_key:"96bulls/history-org/v1/ws=ws/m=srv_fixture/h=claude/d=2026/11/01/s=physical/manifest.json",content_sha256:"\\x"+"a".repeat(64),raw_format:"ndjson",revision:1,trace_id:"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18"});
 await owner.query("SELECT *FROM sessions.upsert_session_index($1::jsonb)",[JSON.stringify(Array.from({length:201},(_,i)=>row(i)))]);
 let ticket:any;const t=()=>({carrierSha256:ticket.carrierSha256,scanToken:ticket.scanToken,leaseToken:ticket.leaseToken,epoch:ticket.epoch});
 await check("GLOBAL reference pages retain201 refs including foreign owner; no limit-as-complete",async()=>{
  ticket=(await call("claim",{bucket:"history-fixture"})).ticket;assert.equal(ticket.referenceCount,"201");let after="0",count=0,foreign=false;while(true){const p=await call("refs",{...t(),after});assert.equal(p.total,"201");assert(p.references.length<=100);count+=p.references.length;foreign ||=p.references.some((r:any)=>r.pointer.user_id==="foreign-user");if(p.next===null)break;after=p.next;}assert.equal(count,201);assert(foreign);
 });
 await check("pointer metadata mutation invalidates original inventory lease even with same raw key",async()=>{
  await owner.query("UPDATE sessions.session_index SET user_id='changed-user' WHERE org_id='foreign-org'");await assert.rejects(()=>call("refs",{...t(),after:"0"}),(e:any)=>e.code==="40001");await owner.query("UPDATE sessions.ingest_history_carriers SET scan_lease_until=clock_timestamp()-interval'1 second'");ticket=(await call("claim",{bucket:"history-fixture"})).ticket;
 });
 const records=[{orgId:"history-org",workspaceId:"ws",originId:"one",recordId:"native",revision:"9007199254740993",count:2}];
 await check("sequential pages are idempotent; changed replay/gap/foreign namespace refuses; exact counters complete",async()=>{
  assert.equal((await call("page",{...t(),page:"0",records})).replay,false);assert.equal((await call("page",{...t(),page:"0",records})).replay,true);
  await assert.rejects(()=>call("page",{...t(),page:"0",records:[{...records[0],revision:"2"}]}),(e:any)=>e.code==="40001");
  await assert.rejects(()=>call("page",{...t(),page:"2",records}),(e:any)=>e.code==="40001");
  await assert.rejects(()=>call("page",{...t(),page:"1",records:[{...records[0],orgId:"absent-org"}]}),(e:any)=>e.code==="22023");
  await assert.rejects(()=>call("finish",{...t(),pages:"1",records:"1",descriptorSha256:"b".repeat(64)}),(e:any)=>e.code==="40001");
  assert.equal((await call("finish",{...t(),pages:"1",records:"2",descriptorSha256:"b".repeat(64)})).complete,true);
  const n=(await owner.query("SELECT max_revision::text,records::text FROM sessions.ingest_history_native_locations")).rows;assert.deepEqual(n,[{max_revision:"9007199254740993",records:"2"}]);
 });
 await check("new raw snapshot invalidates completed native-location inventory",async()=>{
  await owner.query("UPDATE sessions.session_index SET content_sha256=decode(repeat('c',64),'hex') WHERE org_id='history-org' AND session_id='alias0'");const c=(await owner.query("SELECT scan_complete,epoch::text,scanned_epoch::text FROM sessions.ingest_history_carriers")).rows[0];assert.equal(c.scan_complete,false);assert.notEqual(c.epoch,c.scanned_epoch);ticket=(await call("claim",{bucket:"history-fixture"})).ticket;
 });
 await check("expiry AFTER held carrier lock refuses renewal and preserves old deadline",async()=>{
  const lock=new pg.Client(cfg());await lock.connect();await lock.query("BEGIN");await lock.query("UPDATE sessions.ingest_history_carriers SET scan_lease_until=clock_timestamp()+interval'0.15 seconds' WHERE carrier_sha256=$1",[ticket.carrierSha256]);const promise=assert.rejects(()=>call("heartbeat",t()),(e:any)=>e.code==="40001");await new Promise(r=>setTimeout(r,250));await lock.query("COMMIT");await lock.end();await promise;assert.equal((await owner.query("SELECT scan_lease_until<clock_timestamp() AS expired FROM sessions.ingest_history_carriers")).rows[0].expired,true);
 });
 proof.pass=true;
}catch(e:any){proof.pass=false;proof.failure={stage,code:e.code,message:e.message};console.error(proof.failure);process.exitCode=1}
finally{await pool?.end();await owner.end();writeFileSync("/home/khaliqgant/relay-briefs/history-inventory-actual-pg-20261104.json",JSON.stringify(proof,null,2));}
