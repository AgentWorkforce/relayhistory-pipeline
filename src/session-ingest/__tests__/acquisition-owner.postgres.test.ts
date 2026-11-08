/**
 * Factory acquisition owner contract (docs/factory-acquisition-owner-design.md; runbook docs/ingest-acquisition-owner.md)
 * on an OWNED disposable PostgreSQL: loopback, user postgres, no password, scratch databases only.
 * Run: SESSION_INGEST_TEST_PG_URL=postgres://postgres@127.0.0.1:5432/postgres bun test <this file>
 */
import {afterAll,beforeAll,describe,expect,test} from "bun:test";
import pg from "pg";
import {readFileSync,readdirSync,mkdtempSync,rmSync} from "node:fs";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {createHash,createPrivateKey,sign as edSign} from "node:crypto";
import {createFsStore} from "../../../packages/object-store/src/fs.ts";
import {sqlLedger} from "../ledger-sql.ts";
import {deliverBatch} from "../deliver.ts";
import {deliveryAccount} from "../vendor/delivery/prepare.ts";
import {AcquisitionRefusedError} from "../acquisition-refusal.ts";
import {batch,record,TENANT} from "./support.ts";
import {orderedFixtureLedger,initializeFixtureSeatClock} from "./ordered-support.ts";
import type {ObjectStore} from "../../shared/vendor.ts";
import type {ClaimInput,Ledger} from "../ledger.ts";

const url=process.env.SESSION_INGEST_TEST_PG_URL;
if(url){const u=new URL(url);
 if(!["postgresql:","postgres:"].includes(u.protocol)||!["127.0.0.1","localhost","[::1]"].includes(u.hostname)||u.username!=="postgres"
  ||u.password||u.pathname!=="/postgres"||u.search||u.hash)throw Error("owned_loopback_fixture_required");}
const GEN="a".repeat(64);
const root=join(import.meta.dir,"../../..");
const MIGRATION="20261105052000_ingest_acquisition_owner.sql";
const read=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter(l=>!l.startsWith("\\")).join("\n");
const US="\x1f";
const sha=(s:string|Buffer)=>createHash("sha256").update(s).digest("hex");
const G=(c:string)=>"rhg_"+c.repeat(64/c.length);
const grantHash=(org:string,ws:string,src:string,sess:string,id:string,g:string)=>sha(["relayhistory-acquisition-grant-v1",org,ws,src,sess,id,g].join(US));
const FIELDS={
 install:["operationUuid","orgId","workspaceId","source","sessionId","revision","expectedRevision","expectedOwnerEpoch",
  "expectedPredecessorGrantId","ownerKind","ownerUserId","ownerOriginId","bindingUuid","installationId","grantSha256","expiresAt","issuerKeyId"],
 revoke:["operationUuid","orgId","workspaceId","source","sessionId","revision","targetGrantId","targetOwnerEpoch","targetOwnerKind",
  "targetBindingUuid","targetInstallationId","issuerKeyId"],
 rebind:["operationUuid","orgId","workspaceId","source","sessionId","revision","targetGrantId","targetOwnerEpoch","ownerUserId",
  "installationId","oldOriginId","newOriginId","issuerKeyId"]} as const;
type Kind=keyof typeof FIELDS;type P=Record<string,string>;
const assertion=(kind:Kind,p:P)=>["relayhistory-acquisition-assertion-"+kind+"-v1",...FIELDS[kind].map(f=>p[f])].join(US);
/** Signs with a synthetic evidence digest unless one is given; recomputes requestSha256 per §3. */
function seal(kind:Kind,p:P):P{
 const evidenceSha256=p.evidenceSha256??sha("synthetic-evidence"+US+assertion(kind,p));
 return {...Object.fromEntries(FIELDS[kind].map(f=>[f,p[f]])),evidenceSha256,
  requestSha256:sha(["relayhistory-acquisition-request-v1",sha(assertion(kind,p)),evidenceSha256].join(US))};
}
const uuid=()=>crypto.randomUUID();
type Scope={org:string;ws:string;src:string};
function installBody(s:Scope,o:{op?:string;session:string;revision?:string;expectedRevision?:string;expectedOwnerEpoch?:string;pred?:string;
 kind?:"cloud"|"native";user?:string;origin?:string;lineage?:string;g?:string;expiresAt?:string;evidence?:string}):P&{g:string}{
 const op=o.op??uuid(),kind=o.kind??"cloud",lineage=o.lineage??uuid(),g=o.g??G(sha(op).slice(0,16));
 const p=seal("install",{operationUuid:op,orgId:s.org,workspaceId:s.ws,source:s.src,sessionId:o.session,revision:o.revision??"1",
  expectedRevision:o.expectedRevision??"0",expectedOwnerEpoch:o.expectedOwnerEpoch??"0",expectedPredecessorGrantId:o.pred??"",ownerKind:kind,
  ownerUserId:o.user??"user_test",ownerOriginId:o.origin??"origin_test",bindingUuid:kind==="cloud"?lineage:"",installationId:kind==="native"?lineage:"",
  grantSha256:grantHash(s.org,s.ws,s.src,o.session,op,g),expiresAt:o.expiresAt??"",issuerKeyId:"test-key-1",
  ...(o.evidence?{evidenceSha256:o.evidence}:{})});
 return Object.assign(p,{g}) as never;
}
const body=(p:P&{g?:string})=>{const {g:_g,...rest}=p;return rest;};
function revokeBody(s:Scope,o:{op?:string;session:string;revision:string;target:string;epoch:string;kind?:"cloud"|"native";lineage:string}):P{
 const kind=o.kind??"cloud";
 return seal("revoke",{operationUuid:o.op??uuid(),orgId:s.org,workspaceId:s.ws,source:s.src,sessionId:o.session,revision:o.revision,
  targetGrantId:o.target,targetOwnerEpoch:o.epoch,targetOwnerKind:kind,targetBindingUuid:kind==="cloud"?o.lineage:"",
  targetInstallationId:kind==="native"?o.lineage:"",issuerKeyId:"test-key-1"});
}
function rebindBody(s:Scope,o:{op?:string;session:string;revision:string;target:string;epoch:string;user?:string;installation:string;old:string;next:string}):P{
 return seal("rebind",{operationUuid:o.op??uuid(),orgId:s.org,workspaceId:s.ws,source:s.src,sessionId:o.session,revision:o.revision,
  targetGrantId:o.target,targetOwnerEpoch:o.epoch,ownerUserId:o.user??"user_test",installationId:o.installation,oldOriginId:o.old,
  newOriginId:o.next,issuerKeyId:"test-key-1"});
}
const refusal=(sqlstate:string,code:string)=>({code:sqlstate,message:"acquisition: "+code});

type Db={pool:pg.Pool;name:string;dir:string};
let admin:pg.Client|undefined;const dbs:Db[]=[];
async function database(prefix:string,withAcquisition=true):Promise<Db>{
 if(!admin){admin=new pg.Client({connectionString:url});await admin.connect();}
 const name=prefix+"_"+uuid().replaceAll("-","");await admin.query("CREATE DATABASE "+name);
 const u=new URL(url!);u.pathname="/"+name;
 const pool=new pg.Pool({connectionString:u.href,max:12});const dir=mkdtempSync(join(tmpdir(),"acq-"));
 const d={pool,name,dir};dbs.push(d);
 await pool.query(read("supabase/tests/supabase_shim.sql"));await pool.query("CREATE EXTENSION pg_trgm;CREATE EXTENSION btree_gin");
 for(const f of["session_directory_search_worker.sql","session_token_verifier.sql","session_ingest_roles.sql","session_read_api_roles.sql"])
  await pool.query(read("supabase/bootstrap/"+f));
 for(const f of readdirSync(join(root,"supabase/migrations")).sort())if(withAcquisition||f!==MIGRATION)await pool.query(read("supabase/migrations/"+f));
 for(const f of readdirSync(join(root,"src/hoopdb/migrations")).sort())await pool.query(read("src/hoopdb/migrations/"+f));
 return d;
}
afterAll(async()=>{
 for(const d of dbs.splice(0)){await d.pool.end();await admin!.query("DROP DATABASE "+d.name+" WITH (FORCE)");rmSync(d.dir,{recursive:true,force:true});}
 await admin?.end();
},60000);

/** One RPC in its own transaction as `role`; `hold` keeps it open (returns commit/rollback). */
async function rpc(pool:pg.Pool,role:string|null,fn:string,p:unknown,iso=""){
 const c=await pool.connect();
 try{await c.query("BEGIN"+(iso?" ISOLATION LEVEL "+iso:"")+";SET LOCAL statement_timeout=10000"+(role?";SET LOCAL ROLE "+role:""));
  const r=(await c.query("SELECT sessions."+fn+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0].r;await c.query("COMMIT");return r;}
 catch(e){await c.query("ROLLBACK").catch(()=>{});throw e;}finally{c.release();}
}
async function hold(pool:pg.Pool,iso=""){
 const c=await pool.connect();await c.query("BEGIN"+(iso?" ISOLATION LEVEL "+iso:"")+";SET LOCAL statement_timeout=10000");
 const pid=(await c.query("SELECT pg_backend_pid() p")).rows[0].p as number;
 let done=false;const end=async(sql:string)=>{if(done)return;done=true;try{await c.query(sql);}finally{c.release();}};
 return {c,pid,commit:()=>end("COMMIT"),rollback:()=>end("ROLLBACK")};
}
async function waitForBlocker(pool:pg.Pool,waiter:()=>Promise<number|undefined>,blocker:number){
 const end=Date.now()+5000;
 while(Date.now()<end){const w=await waiter();
  if(w!==undefined&&((await pool.query("SELECT pg_blocking_pids($1) p",[w])).rows[0].p as number[]).includes(blocker))return;await Bun.sleep(10);}
 throw Error("expected_fixture_lock_wait_missing");
}
const INSTALLER="bulls96_acquisition_installer";
const TABLES=["switch","hooks_switch","enrollment_gate","enrollment","owners","grants","grant_origins","revoked_grants","revoked_lineages","operations"];
async function acquisitionState(pool:pg.Pool){
 const out:Record<string,unknown>={};
 for(const t of TABLES)out[t]=(await pool.query(`SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text),'[]') j FROM sessions.ingest_acquisition_${t} x`)).rows[0].j;
 return out;
}
const enroll=async(pool:pg.Pool,s:Scope)=>{
 const b=await rpc(pool,null,"ingest_acquisition_enroll_begin",{orgId:s.org,workspaceId:s.ws,source:s.src});
 const c=await rpc(pool,null,"ingest_acquisition_enroll_complete",{orgId:s.org,workspaceId:s.ws,source:s.src});return {b,c};
};
const status=(pool:pg.Pool,s:Scope,ids:string[])=>rpc(pool,INSTALLER,"ingest_acquisition_status",{orgId:s.org,workspaceId:s.ws,source:s.src,sessionIds:ids});

describe("§3 vectors (no database)",()=>{
 test("grant hash and request digest vectors, Ed25519 evidence over A",()=>{
  const g=G("0123456789abcdef"),id="00000000-0000-4000-8000-000000000001";
  expect(grantHash("org_test","ws_test","droid","sess-0001",id,g)).toBe("eca3f7669845e8b736db441074fcbaa51183bfb167f7cc71003d4d564496d227");
  expect(grantHash("org_test","ws_test","droid","sess-0002",id,g)).toBe("359de6c40574725c3256fb529e69c51c4cdefbe06c1cd3558c1068b8bd9019b4");
  const p:P={operationUuid:id,orgId:"org_test",workspaceId:"ws_test",source:"droid",sessionId:"sess-0001",revision:"1",expectedRevision:"0",
   expectedOwnerEpoch:"0",expectedPredecessorGrantId:"",ownerKind:"cloud",ownerUserId:"user_test",ownerOriginId:"origin_test",
   bindingUuid:"00000000-0000-4000-8000-000000000002",installationId:"",grantSha256:"eca3f7669845e8b736db441074fcbaa51183bfb167f7cc71003d4d564496d227",
   expiresAt:"",issuerKeyId:"test-key-1"};
  const a=assertion("install",p);expect(sha(a)).toBe("54e325f1f60abce8e8666d2737de68903a7b4f564fed091e22806f939d3ffc75");
  // RFC 8032 test key 1 (public test vector, not a credential).
  const key=createPrivateKey({key:Buffer.from("302e020100300506032b657004220420"+"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60","hex"),format:"der",type:"pkcs8"});
  const s=edSign(null,Buffer.from(a,"utf8"),key);expect(s.toString("hex").startsWith("fa06607856b9cc7b")).toBe(true);
  const evidence=sha(Buffer.concat([Buffer.from(a,"utf8"),Buffer.from([0x1f]),s]));
  expect(evidence).toBe("9fabebd5cbc741c975fbfd3bb58ea4f127a187240979a1c5ea5960df006138af");
  expect(seal("install",{...p,evidenceSha256:evidence}).requestSha256).toBe("fc501fbafdb4618311ff3ecc20b82e75f0d005e4a652a1a09492931613ebac29");
 });
});

describe.skipIf(!url)("acquisition control plane on actual PostgreSQL",()=>{
 let pool:pg.Pool;
 const S:Scope={org:"org_test",ws:"ws_test",src:"droid"};
 const install=(p:P)=>rpc(pool,INSTALLER,"ingest_acquisition_install",body(p));
 const revoke=(p:P)=>rpc(pool,INSTALLER,"ingest_acquisition_revoke",p);
 const rebind=(p:P)=>rpc(pool,INSTALLER,"ingest_acquisition_rebind_origin",p);
 const zeroWrite=async(work:()=>Promise<unknown>,expected:{code:string;message:string})=>{
  const before=await acquisitionState(pool);await expect(work()).rejects.toMatchObject(expected);expect(await acquisitionState(pool)).toEqual(before);
 };
 beforeAll(async()=>{pool=(await database("acq_control")).pool;},180000);

 test("grants: installer runs install/revoke/rebind/status only; session ingest none; enroll_* and internals no role",async()=>{
  const acl=(await pool.query(`SELECT p.proname,coalesce((SELECT array_agg(DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee)::text END)
    FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.privilege_type='EXECUTE' AND a.grantee<>p.proowner),'{}'::text[]) g
   FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions' AND p.proname LIKE 'ingest\\_acquisition\\_%' ORDER BY 1`)).rows;
  const control=["ingest_acquisition_install","ingest_acquisition_rebind_origin","ingest_acquisition_revoke","ingest_acquisition_status"];
  expect(acl.length).toBeGreaterThan(control.length+2);
  for(const r of acl)expect([r.proname,r.g]).toEqual([r.proname,control.includes(r.proname)?[INSTALLER]:[]]);
  for(const f of["ingest_acquisition_enroll_begin","ingest_acquisition_enroll_complete"])expect(acl.some(r=>r.proname===f)).toBe(true);
  const explicit=(await pool.query(`SELECT count(*)::int n FROM pg_proc p,aclexplode(p.proacl) a WHERE a.grantee=$1::regrole AND a.privilege_type='EXECUTE'`,[INSTALLER])).rows[0].n;
  expect(explicit).toBe(4);
  expect((await pool.query(`SELECT count(*)::int n FROM information_schema.role_table_grants WHERE grantee=$1`,[INSTALLER])).rows[0].n).toBe(0);
  const r=(await pool.query(`SELECT has_schema_privilege($1,'sessions','USAGE') u,has_schema_privilege($1,'sessions','CREATE') c,
   (SELECT rolcanlogin OR rolsuper OR rolinherit OR rolbypassrls OR rolcreaterole OR rolcreatedb FROM pg_roles WHERE rolname=$1) elevated,
   (SELECT bool_or(has_function_privilege('bulls96_session_ingest',p.oid,'EXECUTE')) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
     WHERE n.nspname='sessions' AND p.proname LIKE 'ingest\\_acquisition\\_%') ingest`,[INSTALLER])).rows[0];
  expect(r).toEqual({u:true,c:false,elevated:false,ingest:false});
  await expect(rpc(pool,"bulls96_session_ingest","ingest_acquisition_install",{})).rejects.toMatchObject({code:"42501"});
  await expect(rpc(pool,INSTALLER,"ingest_acquisition_enroll_begin",{orgId:"o",workspaceId:"",source:"droid"})).rejects.toMatchObject({code:"42501"});
 });

 test("dormant defaults: switch off, no enrollment, gate 0; install refuses disabled with zero writes",async()=>{
  expect(await acquisitionState(pool)).toMatchObject({switch:[{singleton:true,install_enabled:false}],hooks_switch:[{singleton:true,enabled:false}],enrollment_gate:[{singleton:true,generation:0}],
   enrollment:[],owners:[],grants:[],grant_origins:[],revoked_grants:[],revoked_lineages:[],operations:[]});
  expect((await pool.query("SELECT count(*)::int n FROM sessions.ingest_claim_attempts WHERE acquisition<>'[]'")).rows[0].n).toBe(0);
  await zeroWrite(()=>install(installBody(S,{session:"dormant-1"})),refusal("55000","disabled"));
  // Role is checked first: a caller that bypasses the EXECUTE grant (the owner) is still not the installer.
  await zeroWrite(()=>rpc(pool,null,"ingest_acquisition_install",body(installBody(S,{session:"dormant-1"}))),refusal("42501","not_installer"));
  await zeroWrite(()=>rpc(pool,null,"ingest_acquisition_status",{orgId:S.org,workspaceId:S.ws,source:S.src,sessionIds:["x"]}),refusal("42501","not_installer"));
 });

 test("hooks flag: off by default; enroll_begin hooks_disabled; set_hooks owner-only, drains readers, refuses disarm while enrolled",async()=>{
  const live=async()=>(await pool.query("SELECT sessions.ingest_acquisition_hooks_live() l")).rows[0].l;
  const setHooks=async(on:boolean|null)=>(await pool.query("SELECT sessions.ingest_acquisition_set_hooks($1) r",[on])).rows[0].r;
  const asRole=async(role:string,sql:string)=>{const c=await hold(pool);try{await c.c.query("SET LOCAL ROLE "+role);return (await c.c.query(sql)).rows;}finally{await c.rollback();}};
  expect(await live()).toBe(false);
  await zeroWrite(()=>rpc(pool,null,"ingest_acquisition_enroll_begin",{orgId:S.org,workspaceId:S.ws,source:S.src}),refusal("55000","hooks_disabled"));
  for(const role of[INSTALLER,"bulls96_session_ingest"])for(const sql of["SELECT sessions.ingest_acquisition_set_hooks(true)","SELECT sessions.ingest_acquisition_hooks_live()"])
   await expect(asRole(role,sql)).rejects.toMatchObject({code:"42501"});
  await expect(setHooks(null)).rejects.toMatchObject(refusal("22023","invalid_request"));
  expect(await setHooks(true)).toBe(true);expect(await setHooks(true)).toBe(true);expect(await live()).toBe(true);
  // A transaction that read the flag holds ACCESS SHARE until it ends: set_hooks waits it out (5s), then refuses.
  const reader=await hold(pool);
  try{await reader.c.query("SELECT sessions.ingest_acquisition_hooks_live()");
   await expect(setHooks(false)).rejects.toMatchObject(refusal("55000","hooks_not_quiescent"));
  }finally{await reader.rollback();}
  // Disarm is refused while any enrollment row exists (here an uncommitted begin, which then rolls back).
  const c=await hold(pool);
  try{expect((await c.c.query("SELECT sessions.ingest_acquisition_enroll_begin($1::jsonb) r",[JSON.stringify({orgId:S.org,workspaceId:S.ws,source:S.src})])).rows[0].r)
    .toEqual({state:"enrolling",generation:"1"});
   await expect(c.c.query("SELECT sessions.ingest_acquisition_set_hooks(false)")).rejects.toMatchObject(refusal("55000","hooks_enrolled"));
  }finally{await c.rollback();}
  expect(await acquisitionState(pool)).toMatchObject({hooks_switch:[{enabled:true}],enrollment_gate:[{generation:0}],enrollment:[]});
  // With no enrollment the flag can go back off and on.
  expect(await setHooks(false)).toBe(false);expect(await live()).toBe(false);expect(await setHooks(true)).toBe(true);
 },30000);

 test("request shape and digest: invalid_request and request_digest_mismatch, byte-exact, zero writes",async()=>{
  const ok=installBody(S,{session:"shape-1"});const bad:Array<Record<string,unknown>>=[
   {...body(ok),extra:"x"},(({issuerKeyId:_,...r})=>r)(body(ok)),{...body(ok),revision:1},{...body(ok),revision:"0"},{...body(ok),revision:"01"},
   {...body(ok),revision:"9223372036854775808"},{...body(ok),operationUuid:ok.operationUuid.toUpperCase()},{...body(ok),sessionId:""},
   {...body(ok),ownerKind:"Cloud"},{...body(ok),installationId:uuid()},{...body(ok),expectedPredecessorGrantId:uuid()},
   {...body(ok),expiresAt:"2026-02-30T00:00:00Z"},{...body(ok),expiresAt:"2026-11-01T00:00:00.000Z"},{...body(ok),grantSha256:"A".repeat(64)},
   {...body(ok),sessionId:"a\u0001b"},{...body(ok),orgId:""}];
  const before=await acquisitionState(pool);const got:string[]=[];
  for(const b of bad)got.push(await rpc(pool,INSTALLER,"ingest_acquisition_install",b).then(()=>"accepted",(e:any)=>e.code+" "+e.message));
  expect(got).toEqual(bad.map(()=>"22023 acquisition: invalid_request"));expect(await acquisitionState(pool)).toEqual(before);
  await zeroWrite(()=>install({...ok,requestSha256:"0".repeat(64)}),refusal("22023","request_digest_mismatch"));
  // A field changed after sealing (here the evidence) is a digest mismatch, never trimmed or re-derived.
  await zeroWrite(()=>install({...ok,evidenceSha256:"1".repeat(64)}),refusal("22023","request_digest_mismatch"));
  for(const b of[{orgId:S.org,workspaceId:S.ws,source:S.src,sessionIds:[]},{orgId:S.org,workspaceId:S.ws,source:S.src,sessionIds:["a","a"]},
   {orgId:S.org,workspaceId:S.ws,source:S.src,sessionIds:["a"],x:1},{orgId:S.org,workspaceId:S.ws,source:S.src,sessionIds:[1]}])
   await expect(rpc(pool,INSTALLER,"ingest_acquisition_status",b)).rejects.toMatchObject(refusal("22023","invalid_request"));
 });

 test("switch on, unenrolled source: not_enrolled; enrolling source still not_enrolled; enroll replay",async()=>{
  await pool.query("UPDATE sessions.ingest_acquisition_switch SET install_enabled=true");
  await zeroWrite(()=>install(installBody(S,{session:"sess-0001"})),refusal("55000","not_enrolled"));
  const before=await acquisitionState(pool);
  expect(await rpc(pool,null,"ingest_acquisition_enroll_begin",{orgId:S.org,workspaceId:S.ws,source:S.src})).toEqual({state:"enrolling",generation:"1"});
  await zeroWrite(()=>install(installBody(S,{session:"sess-0001"})),refusal("55000","not_enrolled"));
  const done=await rpc(pool,null,"ingest_acquisition_enroll_complete",{orgId:S.org,workspaceId:S.ws,source:S.src});
  expect(done).toEqual({state:"enrolled",backfillSha256:sha(""),backfilled:"0",outcome:"enrolled"});
  expect(await rpc(pool,null,"ingest_acquisition_enroll_complete",{orgId:S.org,workspaceId:S.ws,source:S.src})).toEqual({state:"enrolled",backfillSha256:sha(""),outcome:"replay"});
  await expect(rpc(pool,null,"ingest_acquisition_enroll_complete",{orgId:S.org,workspaceId:S.ws,source:"never"})).rejects.toMatchObject(refusal("55000","not_enrolled"));
  await expect(rpc(pool,null,"ingest_acquisition_enroll_begin",{orgId:S.org,workspaceId:S.ws,source:"droid",x:"1"})).rejects.toMatchObject(refusal("22023","invalid_request"));
  expect((await acquisitionState(pool)).owners).toEqual(before.owners);
 });

 test("§3 vector installs in SQL; exact replay; operation_conflict; status truthful with decimal strings",async()=>{
  const id="00000000-0000-4000-8000-000000000001";
  const p=installBody(S,{op:id,session:"sess-0001",lineage:"00000000-0000-4000-8000-000000000002",g:G("0123456789abcdef"),
   evidence:"9fabebd5cbc741c975fbfd3bb58ea4f127a187240979a1c5ea5960df006138af"});
  expect(p.requestSha256).toBe("fc501fbafdb4618311ff3ecc20b82e75f0d005e4a652a1a09492931613ebac29");
  expect((await pool.query("SELECT sessions.ingest_acquisition_grant_hash('org_test','ws_test','droid','sess-0001',$1,$2) h",[id,p.g])).rows[0].h).toBe(p.grantSha256);
  const first=await install(p);expect(first).toEqual({outcome:"installed",grantId:id,ownerEpoch:"1",revision:"1",state:"active"});
  const snapshot=await acquisitionState(pool);
  expect(await install(p)).toEqual({...first,outcome:"replay"});expect(await acquisitionState(pool)).toEqual(snapshot);
  await zeroWrite(()=>install(installBody(S,{op:id,session:"sess-0001",revision:"2",expectedRevision:"1",expectedOwnerEpoch:"1",pred:id})),refusal("40001","operation_conflict"));
  await zeroWrite(()=>revoke(revokeBody(S,{op:id,session:"sess-0001",revision:"2",target:id,epoch:"1",lineage:uuid()})),refusal("40001","operation_conflict"));
  expect((await pool.query("SELECT result FROM sessions.ingest_acquisition_operations WHERE operation_uuid=$1",[id])).rows[0].result)
   .toEqual({outcome:"installed",grantId:id,ownerEpoch:"1",revision:"1",state:"active"});
  expect(await status(pool,S,["sess-0001","absent-1"])).toEqual([
   {sessionId:"sess-0001",state:"active",revision:"1",tombstoneRevision:null,ownerEpoch:"1",ownerKind:"cloud",grantId:id},
   {sessionId:"absent-1",state:null,revision:"0",tombstoneRevision:null,ownerEpoch:"0",ownerKind:null,grantId:null}]);
  expect(JSON.stringify(await status(pool,S,["sess-0001"]))).not.toContain(p.grantSha256);
 });

 test("ordering on an active owner: handover_not_ready, stale_revision, revision_cas_lost, predecessor_mismatch",async()=>{
  const a=installBody(S,{session:"order-1"});await install(a);
  await zeroWrite(()=>install(installBody(S,{session:"order-1",revision:"2",expectedRevision:"1",expectedOwnerEpoch:"1",pred:a.operationUuid})),refusal("55000","handover_not_ready"));
  await zeroWrite(()=>install(installBody(S,{session:"order-1",revision:"1"})),refusal("40001","stale_revision"));
  await zeroWrite(()=>install(installBody(S,{session:"order-1",revision:"4",expectedRevision:"3",expectedOwnerEpoch:"1",pred:a.operationUuid})),refusal("40001","revision_cas_lost"));
  await zeroWrite(()=>install(installBody(S,{session:"order-2",revision:"3",expectedRevision:"2"})),refusal("40001","revision_cas_lost"));
  await zeroWrite(()=>install(installBody(S,{session:"order-2",expectedOwnerEpoch:"1",pred:uuid()})),refusal("40001","predecessor_mismatch"));
 });

 test("owner_unknown: enrollment backfill and the step-8 freshness scan; never free",async()=>{
  const K:Scope={...S,src:"droid-backfill"};
  await pool.query(`INSERT INTO sessions.ingest_record_revisions(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id)
   VALUES($1,$2,'origin-x','rec-1',1,'rec-1:r1',$3,'someone',$4,'old-1')`,[K.org,K.ws,"d".repeat(64),K.src]);
  const {c}=await enroll(pool,K);expect(c).toMatchObject({backfilled:"1",backfillSha256:sha(K.src+US+"old-1")});
  expect((await status(pool,K,["old-1"]))[0]).toMatchObject({state:"unknown",ownerEpoch:"0",revision:"0"});
  await zeroWrite(()=>install(installBody(K,{session:"old-1"})),refusal("55000","owner_unknown"));
  // History that appears after enrollment (no owner row) is caught by the freshness scan.
  await pool.query(`INSERT INTO sessions.ingest_record_revisions(org_id,workspace_id,origin_id,record_id,revision,revision_id,digest,user_id,source,session_id)
   VALUES($1,$2,'origin-x','rec-2',1,'rec-2:r1',$3,'someone',$4,'late-1')`,[K.org,K.ws,"d".repeat(64),K.src]);
  await zeroWrite(()=>install(installBody(K,{session:"late-1"})),refusal("55000","owner_unknown"));
 });

 test("physical_collision: two logical sessions with one physical key, terminal",async()=>{
  const a=installBody(S,{session:"phys:1"});await install(a);
  await zeroWrite(()=>install(installBody(S,{session:"phys_1"})),refusal("55000","physical_collision"));
 });

 test("revoke: target_mismatch, draining with tombstone, revoke-before-install, superseded",async()=>{
  const lineage=uuid();const a=installBody(S,{session:"rv-1",lineage});await install(a);
  for(const wrong of[{epoch:"2"},{kind:"native" as const},{lineage:uuid()}])
   await zeroWrite(()=>revoke(revokeBody(S,{session:"rv-1",revision:"5",target:a.operationUuid,epoch:"1",lineage,...wrong})),refusal("42501","target_mismatch"));
  await zeroWrite(()=>revoke(revokeBody({...S,src:"other"},{session:"rv-1",revision:"5",target:a.operationUuid,epoch:"1",lineage})),refusal("42501","target_mismatch"));
  const r=revokeBody(S,{session:"rv-1",revision:"5",target:a.operationUuid,epoch:"1",lineage});
  expect(await revoke(r)).toEqual({outcome:"draining",revision:"5",ownerEpoch:"1",state:"draining"});
  expect(await revoke(r)).toEqual({outcome:"replay",revision:"5",ownerEpoch:"1",state:"draining"});
  expect((await status(pool,S,["rv-1"]))[0]).toMatchObject({state:"draining",revision:"5",tombstoneRevision:"5",ownerEpoch:"1"});
  // A second revoke of the (now draining) grant is superseded; nothing changes.
  expect(await revoke(revokeBody(S,{session:"rv-1",revision:"6",target:a.operationUuid,epoch:"1",lineage}))).toMatchObject({outcome:"superseded",state:"draining"});
  await zeroWrite(()=>install(installBody(S,{session:"rv-1",revision:"6",expectedRevision:"5",expectedOwnerEpoch:"1",pred:a.operationUuid})),refusal("55000","handover_not_ready"));
  await zeroWrite(()=>install(installBody(S,{session:"rv-1",revision:"5",expectedRevision:"4",expectedOwnerEpoch:"1",pred:a.operationUuid})),refusal("55000","revoked_before_install"));
  // Revoke-before-install: the target uuid never installs, under any revision.
  const pending=uuid(),pl=uuid();
  expect(await revoke(revokeBody(S,{session:"rbi-1",revision:"3",target:pending,epoch:"1",lineage:pl}))).toEqual({outcome:"revoked_pending",revision:"0",ownerEpoch:"0",state:null});
  await zeroWrite(()=>revoke(revokeBody(S,{session:"rbi-1",revision:"3",target:pending,epoch:"2",lineage:pl})),refusal("42501","target_mismatch"));
  await zeroWrite(()=>install(installBody(S,{op:pending,session:"rbi-1",lineage:uuid()})),refusal("55000","revoked_before_install"));
  expect((await status(pool,S,["rbi-1"]))[0]).toMatchObject({state:null,revision:"0"});
 });

 test("§4.9 binding_lineage_delayed_install_after_revoke: I1 disabled, I2 revoked, I1 replay refused, binding C installs",async()=>{
  const B=uuid(),C=uuid();
  await pool.query("UPDATE sessions.ingest_acquisition_switch SET install_enabled=false");
  const i1=installBody(S,{session:"lin-1",lineage:B});await zeroWrite(()=>install(i1),refusal("55000","disabled"));
  await pool.query("UPDATE sessions.ingest_acquisition_switch SET install_enabled=true");
  const i2=installBody(S,{session:"lin-1",lineage:B});await install(i2);
  expect(await revoke(revokeBody(S,{session:"lin-1",revision:"2",target:i2.operationUuid,epoch:"1",lineage:B}))).toMatchObject({outcome:"draining"});
  await zeroWrite(()=>install(i1),refusal("55000","revoked_before_install"));
  expect(await install(installBody(S,{session:"lin-2",lineage:C}))).toMatchObject({outcome:"installed"});
  // Revoked before install: the lineage fence alone refuses a fresh uuid of B on another session.
  await revoke(revokeBody(S,{session:"lin-3",revision:"1",target:uuid(),epoch:"1",lineage:B}));
  await zeroWrite(()=>install(installBody(S,{session:"lin-3",lineage:B})),refusal("55000","revoked_before_install"));
  expect(await install(installBody(S,{session:"lin-3",lineage:C}))).toMatchObject({outcome:"installed"});
 });

 test("§4.9 binding_lineage_revoke_then_install_other_uuid (two connections)",async()=>{
  const B=uuid(),C=uuid();const before=await status(pool,S,["lin-r1"]);
  const c1=await hold(pool);
  try{await c1.c.query("SET LOCAL ROLE "+INSTALLER);
   await c1.c.query("SELECT sessions.ingest_acquisition_revoke($1::jsonb)",[JSON.stringify(revokeBody(S,{session:"lin-r1",revision:"2",target:uuid(),epoch:"1",lineage:B}))]);
   const i1=installBody(S,{session:"lin-r1",lineage:B});
   await zeroWrite(()=>install(i1),refusal("40001","session_busy"));
   await c1.commit();
   await zeroWrite(()=>install(i1),refusal("55000","revoked_before_install"));
   expect(await status(pool,S,["lin-r1"])).toEqual(before);
   expect(await install(installBody(S,{session:"lin-r2",lineage:C}))).toMatchObject({outcome:"installed"});
  }finally{await c1.rollback();}
 });

 test("physical_collision under concurrency: LP busy while the first install is uncommitted, then terminal",async()=>{
  const c1=await hold(pool);
  try{await c1.c.query("SET LOCAL ROLE "+INSTALLER);
   await c1.c.query("SELECT sessions.ingest_acquisition_install($1::jsonb)",[JSON.stringify(body(installBody(S,{session:"pc:2"})))]);
   await zeroWrite(()=>install(installBody(S,{session:"pc_2"})),refusal("40001","session_busy"));
   await c1.commit();
   await zeroWrite(()=>install(installBody(S,{session:"pc_2"})),refusal("55000","physical_collision"));
  }finally{await c1.rollback();}
 });

 test("rebind_origin: same installation, new origin, epoch kept; origin_cas_lost, target_mismatch, stale_revision",async()=>{
  const inst=uuid();const a=installBody(S,{session:"rb-1",kind:"native",lineage:inst,origin:"o1"});await install(a);
  const base={session:"rb-1",target:a.operationUuid,epoch:"1",installation:inst};
  await zeroWrite(()=>rebind(rebindBody(S,{...base,revision:"2",old:"o-other",next:"o2"})),refusal("40001","origin_cas_lost"));
  await zeroWrite(()=>rebind(rebindBody(S,{...base,revision:"2",installation:uuid(),old:"o1",next:"o2"})),refusal("42501","target_mismatch"));
  await zeroWrite(()=>rebind(rebindBody(S,{...base,revision:"2",user:"someone-else",old:"o1",next:"o2"})),refusal("42501","target_mismatch"));
  await zeroWrite(()=>rebind(rebindBody(S,{...base,revision:"1",old:"o1",next:"o2"})),refusal("40001","stale_revision"));
  const ok=rebindBody(S,{...base,revision:"2",old:"o1",next:"o2"});
  expect(await rebind(ok)).toEqual({outcome:"rebound",revision:"2",ownerEpoch:"1"});
  expect(await rebind(ok)).toEqual({outcome:"replay",revision:"2",ownerEpoch:"1"});
  expect((await pool.query("SELECT array_agg(origin_id ORDER BY origin_id) o FROM sessions.ingest_acquisition_grant_origins WHERE grant_id=$1",[a.operationUuid])).rows[0].o).toEqual(["o1","o2"]);
  await zeroWrite(()=>rebind(rebindBody(S,{session:"rb-1",target:installBody(S,{session:"x"}).operationUuid,epoch:"1",installation:inst,revision:"3",old:"o2",next:"o3"})),refusal("42501","target_mismatch"));
 });

 test("bigint revisions round-trip as decimal strings through install, rebind and status",async()=>{
  // A released owner at 2^53 (seeded: no drain producer exists yet) installs at 2^53+1, beyond float precision.
  await pool.query(`INSERT INTO sessions.ingest_acquisition_owners(org_id,workspace_id,source,session_id,physical_source,physical_session,state,revision,owner_epoch)
   VALUES($1,$2,$3,'big-1',$3,'big-1','released',9007199254740992,0)`,[S.org,S.ws,S.src]);
  const inst=uuid();
  const a=installBody(S,{session:"big-1",kind:"native",lineage:inst,revision:"9007199254740993",expectedRevision:"9007199254740992"});
  expect(await install(a)).toEqual({outcome:"installed",grantId:a.operationUuid,ownerEpoch:"1",revision:"9007199254740993",state:"active"});
  expect((await status(pool,S,["big-1"]))[0]).toMatchObject({revision:"9007199254740993",ownerEpoch:"1"});
  expect(await rebind(rebindBody(S,{session:"big-1",target:a.operationUuid,epoch:"1",installation:inst,revision:"9223372036854775807",old:"origin_test",next:"o-max"})))
   .toEqual({outcome:"rebound",revision:"9223372036854775807",ownerEpoch:"1"});
  expect((await status(pool,S,["big-1"]))[0]).toMatchObject({revision:"9223372036854775807"});
  expect((await pool.query("SELECT result->>'revision' r FROM sessions.ingest_acquisition_operations WHERE operation_uuid=$1",[a.operationUuid])).rows[0].r).toBe("9007199254740993");
 });

 test("session_busy: a held LA (writer mid-admission) or enrollment row lock refuses control calls",async()=>{
  const c1=await hold(pool);
  try{await c1.c.query("SELECT sessions.ingest_acquisition_lock($1,$2,$3::jsonb,false)",[S.org,S.ws,JSON.stringify([[S.src,"busy-1"]])]);
   await zeroWrite(()=>install(installBody(S,{session:"busy-1"})),refusal("40001","session_busy"));
   await zeroWrite(()=>revoke(revokeBody(S,{session:"busy-1",revision:"1",target:uuid(),epoch:"1",lineage:uuid()})),refusal("40001","session_busy"));
  }finally{await c1.rollback();}
  const c2=await hold(pool);
  try{await c2.c.query("SELECT 1 FROM sessions.ingest_acquisition_enrollment WHERE org_id=$1 AND source=$2 FOR UPDATE",[S.org,S.src]);
   await zeroWrite(()=>install(installBody(S,{session:"busy-2"})),refusal("40001","session_busy"));
  }finally{await c2.rollback();}
  expect(await install(installBody(S,{session:"busy-1"}))).toMatchObject({outcome:"installed"});
 });

 test("enrollment_not_quiescent: enroll_begin times out behind a held LE",async()=>{
  const c1=await hold(pool);
  try{await c1.c.query("SELECT sessions.ingest_acquisition_gate()");
   const c2=await hold(pool);
   try{await c2.c.query("SET LOCAL lock_timeout='200ms'");
    await expect(c2.c.query("SELECT sessions.ingest_acquisition_enroll_begin($1::jsonb)",[JSON.stringify({orgId:S.org,workspaceId:S.ws,source:"nq"})]))
     .rejects.toMatchObject(refusal("55000","enrollment_not_quiescent"));
   }finally{await c2.rollback();}
  }finally{await c1.rollback();}
  expect((await pool.query("SELECT count(*)::int n FROM sessions.ingest_acquisition_enrollment WHERE source='nq'")).rows[0].n).toBe(0);
 });

 test("§5.1 admission codes (owner-only internals, called directly): every refusal, mixed list, snapshot",async()=>{
  const admit=async(user:string,origin:string,pairs:unknown[],nulls:unknown[],acq:unknown)=>{
   const c=await hold(pool);
   try{await c.c.query("SELECT sessions.ingest_acquisition_lock($1,$2,$3::jsonb,false)",[S.org,S.ws,JSON.stringify(pairs)]);
    return (await c.c.query("SELECT sessions.ingest_acquisition_admit($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb) r",
     [S.org,S.ws,user,origin,JSON.stringify(pairs),JSON.stringify(nulls),acq===undefined?null:JSON.stringify(acq)])).rows[0].r;}
   finally{await c.rollback();}
  };
  const id="00000000-0000-4000-8000-000000000001",g=G("0123456789abcdef");
  const entry=(sess:string,o:Partial<Record<string,string>>={})=>({source:S.src,sessionId:sess,grantId:id,ownerEpoch:"1",grant:g,...o});
  expect(await admit("user_test","origin_test",[[S.src,"sess-0001"]],[],[entry("sess-0001")])).toEqual([{source:S.src,session_id:"sess-0001",owner_epoch:"1",grant_id:id}]);
  expect(await admit("anyone","any",[["unenrolled","s"]],[],undefined)).toEqual([]);
  const detail=async(p:Promise<unknown>)=>{try{await p;}catch(e:any){return {code:e.code,message:e.message,detail:e.detail&&JSON.parse(e.detail)};}throw Error("expected refusal");};
  const one=(sess:string,code:string)=>[{source:S.src,sessionId:sess,code}];
  expect(await detail(admit("user_test","origin_test",[[S.src,"sess-0001"]],[],[entry("sess-0001",{grant:G("f")})]))).toEqual({code:"42501",message:"acquisition: not_owner",detail:one("sess-0001","not_owner")});
  expect(await detail(admit("user_test","origin_test",[[S.src,"sess-0001"]],[],[]))).toMatchObject({detail:one("sess-0001","not_owner")});
  expect(await detail(admit("user_test","other-origin",[[S.src,"sess-0001"]],[],[entry("sess-0001")]))).toMatchObject({detail:one("sess-0001","not_owner")});
  expect(await detail(admit("user_test","origin_test",[[S.src,"sess-0001"]],[],[entry("sess-0001",{ownerEpoch:"2"})]))).toMatchObject({detail:one("sess-0001","not_owner")});
  // Cross-scope vector: sess-0002's hash never admits sess-0001.
  expect(grantHash(S.org,S.ws,S.src,"sess-0002",id,g)).not.toBe(grantHash(S.org,S.ws,S.src,"sess-0001",id,g));
  expect(await detail(admit("someone-else","origin_test",[[S.src,"sess-0001"]],[],[entry("sess-0001")]))).toEqual({code:"55000",message:"acquisition: owner_other_principal",detail:one("sess-0001","owner_other_principal")});
  expect(await detail(admit("user_test","origin_test",[[S.src,"never-seen"]],[],[]))).toMatchObject({code:"55000",detail:one("never-seen","owner_unknown")});
  expect(await detail(admit("user_test","origin_test",[[S.src,"rv-1"]],[],[]))).toMatchObject({detail:one("rv-1","owner_draining")});
  await pool.query(`INSERT INTO sessions.ingest_acquisition_owners(org_id,workspace_id,source,session_id,physical_source,physical_session,state,revision,owner_epoch)
   VALUES($1,$2,$3,'rel-1',$3,'rel-1','released',3,1)`,[S.org,S.ws,S.src]);
  expect(await detail(admit("user_test","origin_test",[[S.src,"rel-1"]],[],[]))).toMatchObject({detail:one("rel-1","owner_released")});
  const exp=installBody(S,{session:"exp-1",expiresAt:"2001-01-01T00:00:00Z"});await install(exp);
  expect(await detail(admit("user_test","origin_test",[[S.src,"exp-1"]],[],[entry("exp-1",{grantId:exp.operationUuid,grant:exp.g})])))
   .toEqual({code:"55000",message:"acquisition: grant_expired",detail:one("exp-1","grant_expired")});
  expect(await detail(admit("user_test","origin_test",[],[S.src,"unenrolled"],[]))).toEqual({code:"22023",message:"acquisition: session_required",detail:one("","session_required")});
  // Mixed: every refused pair is listed; the SQLSTATE is the first's.
  expect(await detail(admit("user_test","origin_test",[[S.src,"rv-1"],[S.src,"sess-0001"],[S.src,"never-seen"]],[S.src],[entry("sess-0001")])))
   .toEqual({code:"55000",message:"acquisition: owner_unknown",detail:[...one("never-seen","owner_unknown"),...one("rv-1","owner_draining"),...one("","session_required")]});
  for(const bad of[[entry("sess-0001",{grant:"rhg_x"})],[{...entry("sess-0001"),extra:"1"}],[entry("sess-0001",{ownerEpoch:"01"})],[entry("sess-0001"),entry("sess-0001")],{}])
   await expect(admit("user_test","origin_test",[[S.src,"sess-0001"]],[],bad)).rejects.toMatchObject(refusal("22023","invalid_request"));
  await expect(admit("user_test","origin_test",[[S.src,"sess-0001"]],[],[entry("other-session")])).rejects.toMatchObject(refusal("22023","acquisition_unexpected"));
  await expect(admit("user_test","origin_test",[["unenrolled","s"]],[],[{...entry("s"),source:"unenrolled"}])).rejects.toMatchObject(refusal("22023","acquisition_unexpected"));
 });

 test("§4.9 install then revoke under a held writer LA: busy, then draining with tombstone = greatest(revision, p.revision)",async()=>{
  const B=uuid();const i1=installBody(S,{session:"lin-h1",lineage:B,revision:"1"});await install(i1);
  const writer=await hold(pool);
  try{await writer.c.query("SELECT sessions.ingest_acquisition_lock($1,$2,$3::jsonb,false)",[S.org,S.ws,JSON.stringify([[S.src,"lin-h1"]])]);
   // Revoke of an absent I2 of the same lineage, and of a superseded grant id: both busy while the writer holds LA.
   await zeroWrite(()=>revoke(revokeBody(S,{session:"lin-h1",revision:"1",target:uuid(),epoch:"1",lineage:B})),refusal("40001","session_busy"));
  }finally{await writer.commit();}
  // p.revision below/at the current revision still drains (never stale_revision for a lineage stop).
  expect(await revoke(revokeBody(S,{session:"lin-h1",revision:"1",target:uuid(),epoch:"1",lineage:B}))).toEqual({outcome:"draining",revision:"1",ownerEpoch:"1",state:"draining"});
  expect((await status(pool,S,["lin-h1"]))[0]).toMatchObject({state:"draining",revision:"1",tombstoneRevision:"1",ownerEpoch:"1",grantId:i1.operationUuid});
  const fenced=(await pool.query("SELECT sessions.ingest_acquisition_fenced($1,$2,$3::jsonb,$4::jsonb,'admit') a,sessions.ingest_acquisition_fenced($1,$2,$3::jsonb,$4::jsonb,'publish') p",
   [S.org,S.ws,JSON.stringify([{source:S.src,session_id:"lin-h1",owner_epoch:"1",grant_id:i1.operationUuid}]),JSON.stringify([[S.src,"lin-h1"]])])).rows[0];
  expect(fenced).toEqual({a:[{source:S.src,sessionId:"lin-h1",code:"owner_changed"}],p:[]});
 });
});

/** Ingest fixture (as publication.postgres.test.ts): buffered delivery through the actual hooks. */
async function ingest(prefix:string,withAcquisition=true){
 const d=await database(prefix,withAcquisition);const pool=d.pool;
 await pool.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
 await pool.query("INSERT INTO sessions.__96bulls_role_validation(name,generation,status)VALUES('session_directory_search_worker',$1,'pass')",[GEN]);
 for(const org of[TENANT.orgId])
  await pool.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)VALUES($1,$2,$2,$3,clock_timestamp()+interval'1 hour',repeat('3',64))",[org,"a".repeat(64),GEN]);
 await initializeFixtureSeatClock(pool,GEN);
 const calls:Array<{fn:string;rows:unknown}>=[];
 let gate:{fn:string;reached:()=>void;release:Promise<void>}|undefined;
 const run=async(t:string,p:unknown[])=>{
  const c=await pool.connect();
  try{await c.query("BEGIN;SET LOCAL statement_timeout=10000;SET LOCAL ROLE bulls96_session_ingest");
   const r=await c.query(t,p as never);const fn=/sessions\.([a-z_]+)\(/.exec(t)?.[1]??"?";calls.push({fn,rows:r.rows});
   if(gate&&fn===gate.fn){const g=gate;gate=undefined;g.reached();await g.release;}
   await c.query("COMMIT");return r;}
  catch(e){await c.query("ROLLBACK").catch(()=>{});throw e;}finally{c.release();}
 };
 const base=sqlLedger(run as never,{acceptanceMode:"buffered",expectedGeneration:GEN});
 const store=createFsStore({root:d.dir}) as unknown as ObjectStore;
 const deliver=async(records:ReturnType<typeof record>[],o:{batch_id:string;tenant?:typeof TENANT;acquisition?:ClaimInput["acquisition"];ledger?:(l:Ledger)=>Ledger})=>{
  const tenant=o.tenant??TENANT;
  let ledger:Ledger=orderedFixtureLedger({...base,claim:(input:ClaimInput)=>base.claim(o.acquisition?{...input,acquisition:o.acquisition}:input)},pool);
  if(o.ledger)ledger=o.ledger(ledger);
  return deliverBatch({ledger,target:{store,keyRoot:"96bulls"},acceptanceMode:"buffered"} as never,tenant,
   await batch(records,{batch_id:o.batch_id,account_id:await deliveryAccount(tenant)}));
 };
 const holdNext=(fn:string)=>{let reached!:()=>void,release!:()=>void;
  const r=new Promise<void>(res=>{reached=res;}),rel=new Promise<void>(res=>{release=res;});gate={fn,reached,release:rel};return {reached:r,release};};
 const frontier=(user=TENANT.userId)=>pool.query("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,'origin-1',$4,$4,clock_timestamp()+interval'1 hour','synthetic','us-east-1','96bulls',$5) ON CONFLICT DO NOTHING",[TENANT.orgId,TENANT.workspaceId,user,"b".repeat(64),GEN]);
 const pubClaim=async(batchId:string)=>{
  const stage=(await pool.query("SELECT active_stage_token token FROM sessions.ingest_buffer_outbox WHERE batch_id=$1",[batchId])).rows[0].token;
  return (await run("SELECT sessions.ingest_publication_claim($1::jsonb) result",[JSON.stringify({stageToken:stage,keyRoot:"96bulls",bucket:"synthetic",region:"us-east-1",generation:GEN})])).rows[0].result;
 };
 const pointer=(o:{harness:string;session:string;machine?:string;revision?:number})=>pool.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)",[JSON.stringify([{
  org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,user_id:TENANT.userId,harness:o.harness,session_id:o.session,machine_id:o.machine??"m-legacy-1",
  created_at:"2026-11-01T00:00:00Z",last_activity_at:"2026-11-01T00:00:00Z",storage_provider:"s3",bucket:"synthetic",
  manifest_key:"96bulls/org-a/v1/ws=ws-a/m="+(o.machine??"m-legacy-1")+"/h="+o.harness+"/d=2026/11/01/s="+o.session+"/manifest.json",
  content_sha256:"\\x"+"f".repeat(64),raw_format:"ndjson",revision:o.revision??1,trace_id:"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18"}])]);
 return {pool,run,calls,deliver,holdNext,frontier,pubClaim,pointer};
}

describe.skipIf(!url)("flag off, and flag on with unenrolled sources, are byte-identical to main through every hooked path",()=>{
 test("same workload: migration with flag off, migration with flag on, no migration: identical RPC results and ledger rows",async()=>{
  // Stage objects embed the per-attempt stage token, so their digest/key differ run to run; mask those and order
  // rows after masking. Record digests, revisions, states, receipts and pointer results stay compared byte-for-byte.
  const normalize=(v:unknown)=>{
   const masked=JSON.parse(JSON.stringify(v,(k,x)=>/^(stage_)?sha256$/.test(k)?"<stage-sha>":x)
    .replace(/b=([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{32}\.staged/g,"b=$1-<stage>.staged")
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g,"<uuid>")
    .replace(/\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}(:\d{2})?)?/g,"<ts>"));
   for(const [k,rows] of Object.entries(masked.tables))if(Array.isArray(rows)&&k!=="pointers")
    rows.sort((a:unknown,b:unknown)=>JSON.stringify(a)<JSON.stringify(b)?-1:1);
   return masked;
  };
  const workload=async(f:Awaited<ReturnType<typeof ingest>>)=>{
   await f.frontier();
   await f.deliver([record({record_id:"bi-1"}),record({record_id:"bi-2",session_id:"sess-2"}),record({record_id:"bi-3",source:"codex"})],{batch_id:"bi-batch-1"});
   await f.deliver([record({record_id:"bi-1",revision:2,revision_id:"bi-1:r2"})],{batch_id:"bi-batch-2"});
   await f.deliver([record({record_id:"bi-1"})],{batch_id:"bi-batch-1"}).catch(e=>({error:String(e?.code??e)}));
   const ticket=await f.pubClaim("bi-batch-1");
   const pointers=[(await f.pointer({harness:"claude",session:"legacy-1"})).rows,(await f.pointer({harness:"claude",session:"legacy-1",revision:2})).rows,
    (await f.pointer({harness:"claude",session:"legacy-1",revision:1})).rows];
   const tables:Record<string,unknown>={ticket,pointers};
   for(const t of["ingest_batches","ingest_claim_attempts","ingest_session_ownership","ingest_record_revisions","ingest_publication_heads","session_index","ingest_stage_intents","ingest_buffer_outbox"])
    tables[t]=(await f.pool.query(`SELECT coalesce(jsonb_agg(to_jsonb(x)-'acquisition'-'acquisition_epoch' ORDER BY (to_jsonb(x)-'acquisition'-'acquisition_epoch')::text),'[]') j FROM sessions.${t} x`)).rows[0].j;
   return normalize({calls:f.calls,tables});
  };
  const withMigration=await workload(await ingest("acq_identity_a",true));
  const armed=await ingest("acq_identity_c",true);await armed.pool.query("SELECT sessions.ingest_acquisition_set_hooks(true)");
  const withFlag=await workload(armed);
  const main=await workload(await ingest("acq_identity_b",false));
  expect(new Set((withMigration as any).calls.map((c:any)=>c.fn))).toEqual(new Set(["ingest_claim_buffered_batch","ingest_prepare_buffer_stage",
   "ingest_commit_buffered_batch","ingest_publication_claim"]));
  const t=(withMigration as any).tables;
  expect(t.pointers.map((r:any)=>r.map((x:any)=>x.applied))).toEqual([[true],[true],[false]]);
  for(const k of["ingest_batches","ingest_claim_attempts","ingest_session_ownership","ingest_publication_heads","session_index","ingest_stage_intents"])
   expect(t[k].length).toBeGreaterThan(0);
  expect(withMigration).toEqual(main);
  expect(withFlag).toEqual(main);
 },360000);

 test("flag off: hooks take no gate (writes proceed while the gate row is locked); acquisition key stays malformed; enroll_begin hooks_disabled",async()=>{
  const f=await ingest("acq_flag_off");await f.frontier();
  const g=await hold(f.pool);
  try{await g.c.query("SELECT 1 FROM sessions.ingest_acquisition_enrollment_gate FOR UPDATE");
   expect((await f.deliver([record({record_id:"ff-1"})],{batch_id:"ff-batch-1"})).outcome).toBe("accepted_buffered");
   expect((await f.pointer({harness:"claude",session:"ff-legacy-1"})).rows[0].applied).toBe(true);
   expect((await f.pubClaim("ff-batch-1")).heads.length).toBeGreaterThan(0);
   // Control: armed, the same held gate row makes the claim busy (503), so the assertions above are meaningful.
   await f.pool.query("SELECT sessions.ingest_acquisition_set_hooks(true)");
   const busy=await f.deliver([record({record_id:"ff-2",session_id:"ff-s-2"})],{batch_id:"ff-batch-2"}).then(()=>undefined,x=>x);
   expect(busy).not.toBeInstanceOf(AcquisitionRefusedError);expect(busy).toMatchObject({status:503});
   await f.pool.query("SELECT sessions.ingest_acquisition_set_hooks(false)");
  }finally{await g.rollback();}
  const e=await f.deliver([record({record_id:"ff-3",session_id:"ff-s-3"})],{batch_id:"ff-batch-3",
   acquisition:[{source:"claude",sessionId:"ff-s-3",grantId:uuid(),ownerEpoch:"1",grant:G("a")}]}).then(()=>undefined,x=>x);
  expect(e).toBeDefined();expect(e).not.toBeInstanceOf(AcquisitionRefusedError);
  expect((await f.pool.query("SELECT count(*)::int n FROM sessions.ingest_claim_attempts WHERE batch_id='ff-batch-3'")).rows[0].n).toBe(0);
  await expect(rpc(f.pool,null,"ingest_acquisition_enroll_begin",{orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,source:"claude"}))
   .rejects.toMatchObject(refusal("55000","hooks_disabled"));
  expect((await f.pool.query("SELECT count(*)::int n FROM sessions.ingest_acquisition_enrollment")).rows[0].n).toBe(0);
 },240000);
});

describe.skipIf(!url)("enrolled source through the actual hooks (edge 409 refusals)",()=>{
 let f:Awaited<ReturnType<typeof ingest>>;
 const S:Scope={org:TENANT.orgId,ws:TENANT.workspaceId,src:"claude"};
 const owner=async(session:string,o:{user?:string;origin?:string}={})=>{
  const p=installBody(S,{session,user:o.user??TENANT.userId,origin:o.origin??"origin-1"});
  await rpc(f.pool,INSTALLER,"ingest_acquisition_install",body(p));
  return {p,entry:{source:S.src,sessionId:session,grantId:p.operationUuid,ownerEpoch:"1",grant:p.g}};
 };
 const rows=async()=>(await f.pool.query("SELECT (SELECT count(*) FROM sessions.ingest_batches)::int b,(SELECT count(*) FROM sessions.ingest_claim_attempts)::int a,(SELECT count(*) FROM sessions.ingest_stage_intents)::int s")).rows[0];
 const refused=async(p:Promise<unknown>,list:unknown[])=>{
  const before=await rows();const e=await p.then(()=>undefined,x=>x);
  expect(e).toBeInstanceOf(AcquisitionRefusedError);expect({status:e.status,code:e.code,refused:e.refused}).toEqual({status:409,code:"acquisition_refused",refused:list});
  expect(await rows()).toEqual(before);
 };
 beforeAll(async()=>{f=await ingest("acq_hooks");await f.frontier();
  await f.pool.query("SELECT sessions.ingest_acquisition_set_hooks(true)");
  await rpc(f.pool,null,"ingest_acquisition_enroll_begin",{orgId:S.org,workspaceId:S.ws,source:S.src});
  await rpc(f.pool,null,"ingest_acquisition_enroll_complete",{orgId:S.org,workspaceId:S.ws,source:S.src});
  await f.pool.query("UPDATE sessions.ingest_acquisition_switch SET install_enabled=true");
 },180000);

 test("no owner row: owner_unknown 409, zero writes; unenrolled codex in the same org still accepted",async()=>{
  await refused(f.deliver([record({record_id:"e-1",session_id:"h-unknown"})],{batch_id:"e-batch-1"}),[{source:"claude",sessionId:"h-unknown",code:"owner_unknown"}]);
  expect((await f.deliver([record({record_id:"e-2",source:"codex",session_id:"c-1"})],{batch_id:"e-batch-2"})).outcome).toBe("accepted_buffered");
 },30000);

 test("granted owner: no grant or wrong G is not_owner; second user owner_other_principal; exact grant admits and publishes at its epoch",async()=>{
  const {entry}=await owner("h-1");
  await refused(f.deliver([record({record_id:"g-1",session_id:"h-1"})],{batch_id:"g-batch-1"}),[{source:"claude",sessionId:"h-1",code:"not_owner"}]);
  await refused(f.deliver([record({record_id:"g-1",session_id:"h-1"})],{batch_id:"g-batch-1",acquisition:[{...entry,grant:G("e")}]}),[{source:"claude",sessionId:"h-1",code:"not_owner"}]);
  const other={...TENANT,userId:"user-2"};
  await refused(f.deliver([record({record_id:"g-2",session_id:"h-1"})],{batch_id:"g-batch-2",tenant:other,acquisition:[entry]}),[{source:"claude",sessionId:"h-1",code:"owner_other_principal"}]);
  expect((await f.deliver([record({record_id:"g-1",session_id:"h-1"})],{batch_id:"g-batch-1",acquisition:[entry]})).outcome).toBe("accepted_buffered");
  expect((await f.pool.query("SELECT acquisition FROM sessions.ingest_claim_attempts WHERE batch_id='g-batch-1'")).rows[0].acquisition)
   .toEqual([{source:"claude",session_id:"h-1",owner_epoch:"1",grant_id:entry.grantId}]);
  const ticket=await f.pubClaim("g-batch-1");
  expect(ticket.heads.map((h:any)=>h.acquisitionEpoch)).toEqual(["1"]);
  expect((await f.pool.query("SELECT acquisition_epoch::text e FROM sessions.ingest_publication_heads WHERE session_id='h-1'")).rows[0].e).toBe("1");
 },30000);

 test("enrolled record without a session id: session_required with sessionId \"\"",async()=>{
  await refused(f.deliver([record({record_id:"n-1",session_id:null as never,kind:"session_event"})],{batch_id:"n-batch-1"}).catch(e=>{
   if(e instanceof AcquisitionRefusedError)throw e;return Promise.reject(e);}),[{source:"claude",sessionId:"",code:"session_required"}]);
 },30000);

 test("admission ↔ revoke (two connections): claim mid-admission makes revoke busy; after it commits, revoke drains, new claim owner_draining, accepted receipt still publishes",async()=>{
  const B=uuid();const p=installBody(S,{session:"h-2",user:TENANT.userId,origin:"origin-1",lineage:B});
  await rpc(f.pool,INSTALLER,"ingest_acquisition_install",body(p));
  const entry={source:"claude",sessionId:"h-2",grantId:p.operationUuid,ownerEpoch:"1",grant:p.g};
  const h=f.holdNext("ingest_claim_buffered_batch");
  const delivering=f.deliver([record({record_id:"a-1",session_id:"h-2"})],{batch_id:"a-batch-1",acquisition:[entry]});
  await h.reached;
  const r=revokeBody(S,{session:"h-2",revision:"2",target:uuid(),epoch:"1",lineage:B});
  await expect(rpc(f.pool,INSTALLER,"ingest_acquisition_revoke",r)).rejects.toMatchObject(refusal("40001","session_busy"));
  await expect(rpc(f.pool,INSTALLER,"ingest_acquisition_install",body(installBody(S,{session:"h-2",revision:"2",expectedRevision:"1",expectedOwnerEpoch:"1",pred:p.operationUuid})))).rejects.toMatchObject(refusal("40001","session_busy"));
  h.release();expect((await delivering).outcome).toBe("accepted_buffered");
  expect(await rpc(f.pool,INSTALLER,"ingest_acquisition_revoke",r)).toMatchObject({outcome:"draining",ownerEpoch:"1"});
  await refused(f.deliver([record({record_id:"a-2",session_id:"h-2"})],{batch_id:"a-batch-2",acquisition:[entry]}),[{source:"claude",sessionId:"h-2",code:"owner_draining"}]);
  const ticket=await f.pubClaim("a-batch-1");expect(ticket.heads.map((x:any)=>x.acquisitionEpoch)).toEqual(["1"]);
 },30000);

 test("install ↔ admission (two connections): an uncommitted install makes the claim busy (503), never a 409",async()=>{
  const p=installBody(S,{session:"h-3",user:TENANT.userId,origin:"origin-1"});
  const c=await hold(f.pool);
  try{await c.c.query("SET LOCAL ROLE "+INSTALLER);await c.c.query("SELECT sessions.ingest_acquisition_install($1::jsonb)",[JSON.stringify(body(p))]);
   const e=await f.deliver([record({record_id:"i-1",session_id:"h-3"})],{batch_id:"i-batch-1"}).then(()=>undefined,x=>x);
   expect(e).not.toBeInstanceOf(AcquisitionRefusedError);expect(e).toMatchObject({status:503});
  }finally{await c.commit();}
  expect((await f.deliver([record({record_id:"i-1",session_id:"h-3"})],{batch_id:"i-batch-1",acquisition:[{source:"claude",sessionId:"h-3",grantId:p.operationUuid,ownerEpoch:"1",grant:p.g}]})).outcome).toBe("accepted_buffered");
 },30000);

 test("buffered stage after revoke: admitted claim, then revoke, then prepare → owner_changed 409, nothing staged",async()=>{
  const B=uuid();const p=installBody(S,{session:"h-4",user:TENANT.userId,origin:"origin-1",lineage:B});
  await rpc(f.pool,INSTALLER,"ingest_acquisition_install",body(p));
  const entry={source:"claude",sessionId:"h-4",grantId:p.operationUuid,ownerEpoch:"1",grant:p.g};
  const stagesBefore=(await f.pool.query("SELECT count(*)::int n FROM sessions.ingest_stage_intents")).rows[0].n;
  const e=await f.deliver([record({record_id:"s-1",session_id:"h-4"})],{batch_id:"s-batch-1",acquisition:[entry],ledger:l=>({...l,
   prepareBufferStage:async(input)=>{await rpc(f.pool,INSTALLER,"ingest_acquisition_revoke",revokeBody(S,{session:"h-4",revision:"2",target:p.operationUuid,epoch:"1",lineage:B}));return l.prepareBufferStage!(input);}})}).then(()=>undefined,x=>x);
  expect(e).toBeInstanceOf(AcquisitionRefusedError);expect(e.refused).toEqual([{source:"claude",sessionId:"h-4",code:"owner_changed"}]);
  expect((await f.pool.query("SELECT count(*)::int n FROM sessions.ingest_stage_intents")).rows[0].n).toBe(stagesBefore);
 },30000);

 test("claim ↔ enroll (two connections): claim mid-admission makes enroll_begin not_quiescent; uncommitted begin makes the claim busy (503), never a 409",async()=>{
  const enrolled=async()=>(await f.pool.query("SELECT count(*)::int n FROM sessions.ingest_acquisition_enrollment WHERE source='codex'")).rows[0].n;
  const begin=JSON.stringify({orgId:S.org,workspaceId:S.ws,source:"codex"});
  const h=f.holdNext("ingest_claim_buffered_batch");
  const delivering=f.deliver([record({record_id:"ce-1",source:"codex",session_id:"ce-s-1"})],{batch_id:"ce-batch-1"});
  await h.reached;
  const c=await hold(f.pool);
  try{await c.c.query("SET LOCAL lock_timeout='200ms'");
   await expect(c.c.query("SELECT sessions.ingest_acquisition_enroll_begin($1::jsonb)",[begin])).rejects.toMatchObject(refusal("55000","enrollment_not_quiescent"));
  }finally{await c.rollback();}
  h.release();expect((await delivering).outcome).toBe("accepted_buffered");
  expect(await enrolled()).toBe(0);
  const c2=await hold(f.pool);
  try{expect((await c2.c.query("SELECT sessions.ingest_acquisition_enroll_begin($1::jsonb) r",[begin])).rows[0].r).toMatchObject({state:"enrolling"});
   const e=await f.deliver([record({record_id:"ce-2",source:"codex",session_id:"ce-s-2"})],{batch_id:"ce-batch-2"}).then(()=>undefined,x=>x);
   expect(e).not.toBeInstanceOf(AcquisitionRefusedError);expect(e).toMatchObject({status:503});
  }finally{await c2.rollback();}
  expect(await enrolled()).toBe(0);
  expect((await f.deliver([record({record_id:"ce-2",source:"codex",session_id:"ce-s-2"})],{batch_id:"ce-batch-2"})).outcome).toBe("accepted_buffered");
 },30000);

 test("commit ↔ owner change: revoke before commit → owner_changed 409, nothing committed; commit mid-flight makes revoke busy, then both succeed in order",async()=>{
  const committed=async(b:string)=>(await f.pool.query("SELECT (SELECT count(*) FROM sessions.ingest_buffer_outbox WHERE batch_id=$1)::int o,(SELECT count(*) FROM sessions.ingest_batches WHERE batch_id=$1 AND state='committed')::int b",[b])).rows[0];
  const B=uuid();const p=installBody(S,{session:"h-5",user:TENANT.userId,origin:"origin-1",lineage:B});
  await rpc(f.pool,INSTALLER,"ingest_acquisition_install",body(p));
  const entry={source:"claude",sessionId:"h-5",grantId:p.operationUuid,ownerEpoch:"1",grant:p.g};
  const e=await f.deliver([record({record_id:"cm-1",session_id:"h-5"})],{batch_id:"cm-batch-1",acquisition:[entry],ledger:l=>({...l,
   commitBuffered:async(input)=>{await rpc(f.pool,INSTALLER,"ingest_acquisition_revoke",revokeBody(S,{session:"h-5",revision:"2",target:p.operationUuid,epoch:"1",lineage:B}));return l.commitBuffered!(input);}})}).then(()=>undefined,x=>x);
  expect(e).toBeInstanceOf(AcquisitionRefusedError);expect({status:e.status,refused:e.refused}).toEqual({status:409,refused:[{source:"claude",sessionId:"h-5",code:"owner_changed"}]});
  expect(await committed("cm-batch-1")).toEqual({o:0,b:0});
  const B2=uuid();const q=installBody(S,{session:"h-6",user:TENANT.userId,origin:"origin-1",lineage:B2});
  await rpc(f.pool,INSTALLER,"ingest_acquisition_install",body(q));
  const h=f.holdNext("ingest_commit_buffered_batch");
  const delivering=f.deliver([record({record_id:"cm-2",session_id:"h-6"})],{batch_id:"cm-batch-2",
   acquisition:[{source:"claude",sessionId:"h-6",grantId:q.operationUuid,ownerEpoch:"1",grant:q.g}]});
  await h.reached;
  const r=revokeBody(S,{session:"h-6",revision:"2",target:q.operationUuid,epoch:"1",lineage:B2});
  await expect(rpc(f.pool,INSTALLER,"ingest_acquisition_revoke",r)).rejects.toMatchObject(refusal("40001","session_busy"));
  h.release();expect((await delivering).outcome).toBe("accepted_buffered");
  expect(await committed("cm-batch-2")).toEqual({o:1,b:1});
  expect(await rpc(f.pool,INSTALLER,"ingest_acquisition_revoke",r)).toMatchObject({outcome:"draining",ownerEpoch:"1"});
 },30000);

 test("§4.8 enrollment_interlock_writer_during_uncommitted_begin (both orders) on the legacy pointer guard",async()=>{
  const c1=await hold(f.pool);
  try{await c1.c.query("SELECT sessions.ingest_acquisition_enroll_begin($1::jsonb)",[JSON.stringify({orgId:S.org,workspaceId:S.ws,source:"gemini"})]);
   await expect(f.pointer({harness:"gemini",session:"il-1"})).rejects.toMatchObject(refusal("40001","session_busy"));
  }finally{await c1.rollback();}
  expect((await f.pool.query("SELECT count(*)::int n FROM sessions.session_index WHERE harness='gemini'")).rows[0].n).toBe(0);
  // Reversed: the writer locks LE first; begin waits for its commit, so the backfill sees it, and its next write is refused.
  const c2=await hold(f.pool);let c1pid:number|undefined;
  const c3=await f.pool.connect();
  try{await c2.c.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)",[JSON.stringify([{org_id:S.org,workspace_id:S.ws,user_id:TENANT.userId,harness:"gemini",session_id:"il-2",
    machine_id:"m-legacy-1",created_at:"2026-11-01T00:00:00Z",last_activity_at:"2026-11-01T00:00:00Z",storage_provider:"s3",bucket:"synthetic",
    manifest_key:"96bulls/org-a/v1/ws=ws-a/m=m-legacy-1/h=gemini/d=2026/11/01/s=il-2/manifest.json",content_sha256:"\\x"+"f".repeat(64),raw_format:"ndjson",revision:1,
    trace_id:"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18"}])]);
   c1pid=(await c3.query("SELECT pg_backend_pid() p")).rows[0].p;
   const begin=c3.query("SELECT sessions.ingest_acquisition_enroll_begin($1::jsonb) r",[JSON.stringify({orgId:S.org,workspaceId:S.ws,source:"gemini"})]);
   await waitForBlocker(f.pool,async()=>c1pid,c2.pid);
   await c2.commit();expect((await begin).rows[0].r).toMatchObject({state:"enrolling"});
  }finally{await c2.rollback();c3.release();}
  expect((await rpc(f.pool,null,"ingest_acquisition_enroll_complete",{orgId:S.org,workspaceId:S.ws,source:"gemini"}))).toMatchObject({backfilled:"1"});
  expect((await f.pool.query("SELECT state FROM sessions.ingest_acquisition_owners WHERE source='gemini' AND session_id='il-2'")).rows[0].state).toBe("unknown");
  expect((await f.pointer({harness:"gemini",session:"il-2",revision:2})).rows[0].applied).toBe(false);
  expect((await f.pool.query("SELECT revision::int r FROM sessions.session_index WHERE harness='gemini' AND session_id='il-2'")).rows[0].r).toBe(1);
 },30000);

 test("§4.8 enrollment_interlock_retained_repeatable_read_snapshot: native 40001, nothing written; retry refused",async()=>{
  const c2=await hold(f.pool,"REPEATABLE READ");
  try{await c2.c.query("SELECT 1");
   await rpc(f.pool,null,"ingest_acquisition_enroll_begin",{orgId:S.org,workspaceId:S.ws,source:"kiro"});
   await expect(c2.c.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)",[JSON.stringify([{org_id:S.org,workspace_id:S.ws,user_id:TENANT.userId,harness:"kiro",session_id:"rr-1",
    machine_id:"m-legacy-1",created_at:"2026-11-01T00:00:00Z",last_activity_at:"2026-11-01T00:00:00Z",storage_provider:"s3",bucket:"synthetic",
    manifest_key:"96bulls/org-a/v1/ws=ws-a/m=m-legacy-1/h=kiro/d=2026/11/01/s=rr-1/manifest.json",content_sha256:"\\x"+"f".repeat(64),raw_format:"ndjson",revision:1,
    trace_id:"019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18"}])])).rejects.toMatchObject({code:"40001"});
  }finally{await c2.rollback();}
  expect((await f.pool.query("SELECT count(*)::int n FROM sessions.session_index WHERE harness='kiro'")).rows[0].n).toBe(0);
  expect((await f.pointer({harness:"kiro",session:"rr-1"})).rows[0].applied).toBe(false);
  expect((await f.pool.query("SELECT count(*)::int n FROM sessions.session_index WHERE harness='kiro'")).rows[0].n).toBe(0);
 },30000);
});
