/**
 * 20261105043860 sessions.buffered_search_gate_for: the read Worker's ONLY way to evaluate the
 * buffered search gate. Seeds through the real buffered acceptance path (the 043700 fixture:
 * POST /v1/turns/batches, frozen-supabase custody, lazy-current policy), then calls the reader as
 * bulls96_read_api with the transaction-local bulls96_read.* pins the read API sets, passing the
 * owner's R2 control-head tuple. Every Dogpatch negative (P0-1 stale head/baseline/generation/
 * deletion/reset/lineage, P0-2 cross-origin deletions, P1 claim-to-session binding) drops the hit.
 */
import {test,expect,afterEach,setDefaultTimeout} from "bun:test";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {sqlLedger} from "../../session-ingest/ledger-sql.ts";
import {deliveryAccount} from "../../session-ingest/vendor/delivery/prepare.ts";
import {DELIVERY_MAPPING_VERSION} from "../../session-ingest/vendor/delivery/delivery-contracts.ts";
import {TURN_KIND,TURN_ORIGIN_ID,turnRecordId} from "../../session-ingest/turns.ts";
import {TENANT,scratchStore} from "../../session-ingest/__tests__/support.ts";
import {SESSION_BOOTSTRAP_FILES} from "../../hoopdb/session-bootstrap-chain.ts";
import {GATE_SQL,gateHeadParams,parseControlHead} from "../buffered-search-gate.ts";
setDefaultTimeout(120000);

const root=join(import.meta.dir,"../../.."),generation="4".repeat(64),epoch="1".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
const E1=epoch,E2="2".repeat(64),D3="3".repeat(64),D2="2".repeat(64),G8="8".repeat(64),D9="9".repeat(64);
/** psql meta-commands (\set) are not SQL. */
const psql=(p:string)=>readFileSync(join(root,p),"utf8").split("\n").filter((l)=>!l.startsWith("\\")).join("\n");
const open:Array<()=>Promise<unknown>|void>=[];
afterEach(async()=>{for(const close of open.splice(0).reverse())await close()});

async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());const read=(p:string)=>readFileSync(join(root,p),"utf8");
 await db.exec(read("supabase/tests/supabase_shim.sql"));
 // Single-sourced bootstrap chain: the grant migration FAILs closed without the
 // bare consumer grantee, so provision it exactly like migrate's bootstrap step.
 for(const f of SESSION_BOOTSTRAP_FILES)await db.exec(read(f));
 for(const f of readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort())await db.exec(read("supabase/migrations/"+f));
 // The read role's approved shape (schema USAGE, the session_index column grant, RLS), as the operator applies it.
 await db.exec(psql("supabase/bootstrap/session_read_api_role.sql"));
 await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','"+generation+"','pass')");
 const owner=async(name:string,p:unknown)=>(await db.query("SELECT sessions."+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0]!.r as any;
 await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation,sourceIdentitySha256:identity,controlEpoch:epoch,privacyBaselineSha256:baseline,retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
 await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:"9007199254740993",isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:"9007199254740993",generation,sourceIdentitySha256:identity,sourceProofSha256:"d".repeat(64)});
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation});
 const fs=scratchStore();open.push(fs.done);const deps={ledger,acceptanceMode:"buffered" as const,target:{store:fs.store,keyRoot:"96bulls"}};
 await db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",[identity,epoch,baseline,"e".repeat(64)]);
 const {createIngestApp}=await import("../../session-ingest/app.ts");
 let who:Tenant=TENANT;
 const app=createIngestApp({identify:()=>({...who,scopes:["rth:sync"]}),deliver:deps,log:()=>{},custodySource:"frozen-supabase"});
 let serial=0;
 /** One accepted buffered turn batch for (source, session) as tenant t (default TENANT); returns its batch id. */
 async function upload(session:string,turns=2,source="claude",start=0,t:Tenant=TENANT){
  const batchId="b-"+(++serial);who=t;
  const records=Array.from({length:turns},(_,j)=>{const i=start+j;
   const record_id=turnRecordId(t.workspaceId,source,session,i),revision=1_793_000_000_000_000+serial*100+i;
   return {schema_version:1 as const,origin_id:TURN_ORIGIN_ID,record_id,revision_id:`turnrev:${record_id.slice(5)}:${revision}`,revision,kind:TURN_KIND as any,source,session_id:session,operation:"upsert" as const,
    payload:{id:`${String(serial*100+i).padStart(8,"0")}-aaaa-4bbb-8ccc-dddddddddddd`,org_id:t.orgId,workspace_id:t.workspaceId,source,session_id:session,turn_index:i,session_owner:"user-1",role:"user",content:`turn ${i}`,actor_name:"Ada",actor_role:"owner",metadata_json:"{}",ts:"2026-11-05T12:00:00.123Z",tagged:true}};
  });
  const b={schema_version:1 as const,origin_id:TURN_ORIGIN_ID,batch_id:batchId,job_id:"relayhistory-turns",generation:1,destination_id:"relayhistory",instance_id:"relayhistory-api",account_id:await deliveryAccount(t),mapping_version:DELIVERY_MAPPING_VERSION,records};
  const r=await app.request("/v1/turns/batches",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({protocolVersion:1,batch:b})});
  who=TENANT;
  if(r.status!==200)throw new Error("upload "+r.status+" "+await r.text());
  return batchId;
 }
 /** Run sql as `role` in a READ ONLY transaction with the read API's tenant pins (null = unset).
  *  Default: a genuine login of `role` (SET SESSION AUTHORIZATION: session_user = role, role GUC 'none'),
  *  as the Worker's Hyperdrive connection is. With `via`, the session is the login `via` and the
  *  transaction then runs SET LOCAL ROLE `role` (a member assuming the role). PGlite does not restore
  *  session_user on RESET or ROLLBACK, so the superuser is reinstated explicitly. */
 const asReader=async(sql:string,params:unknown[]=[],bind:{org?:string|null;ws?:string|null}={},role="bulls96_read_api",via?:string)=>{
  await db.exec("SET SESSION AUTHORIZATION "+(via??role));
  try{
   await db.exec("BEGIN READ ONLY");
   try{
    const org=bind.org===undefined?TENANT.orgId:bind.org,ws=bind.ws===undefined?TENANT.workspaceId:bind.ws;
    if(org!==null)await db.query("SELECT set_config('bulls96_read.org_id',$1,true)",[org]);
    if(ws!==null)await db.query("SELECT set_config('bulls96_read.workspace_id',$1,true)",[ws]);
    if(via!==undefined)await db.exec("SET LOCAL ROLE "+role);
    const r=await db.query(sql,params as never);await db.exec("COMMIT");return r.rows as any[];
   }catch(e){await db.exec("ROLLBACK");throw e}
  }finally{await db.exec("SET SESSION AUTHORIZATION postgres")}
 };
 const hp=(h:Head)=>[h.cutover,h.epoch,h.version,h.digest,h.kind,h.pending,h.baseline];
 /** The reader for the owner's hits, exactly as the Worker calls it (GATE_SQL). */
 const gate=async(hits:Hit[]|unknown,h:Head,o:{owner?:string;org?:string;ws?:string;bind?:{org?:string|null;ws?:string|null};role?:string;via?:string}={})=>
  asReader(GATE_SQL,[o.org??TENANT.orgId,o.ws??TENANT.workspaceId,o.owner??TENANT.userId,...hp(h),JSON.stringify(hits)],o.bind??{},o.role,o.via);
 /** The verdict of one hit (undefined = no row). */
 const verdict=async(hit:Hit,h:Head,o={})=>{const r=await gate([hit],h,o);expect(r.length).toBeLessThanOrEqual(1);return r.length?r[0].drop_reason as string|null:undefined};
 /** The claim token whose immutable records hold (source, session). */
 const claimOf=async(session:string,source="claude")=>((await db.query("SELECT claim_token::text t FROM sessions.ingest_claim_attempts WHERE records @> jsonb_build_array(jsonb_build_object('source',$1::text,'session_id',$2::text))",[source,session])).rows[0] as any).t as string;
 const hit=async(session:string,source="claude"):Promise<Hit>=>({claim_token:await claimOf(session,source),source,session_id:session});
 /** Test-only raw owner G2 row write (delete + insert, so negative tests can plant any row; NOT the production
  *  writer contract in the migration header, which is CAS, never a blind upsert). Digests come from the canonical
  *  helper unless o overrides a field. */
 const prove=async(h:Head=NONE,o:{org?:string;ws?:string;user?:string;generation?:string;deletion?:string;reset?:string;lineage?:string;rowHeadDigest?:string;rowHeadKind?:string|null;baseline?:string}={})=>{
  const key=[o.org??TENANT.orgId,o.ws??TENANT.workspaceId,o.user??TENANT.userId];
  await db.exec("BEGIN");
  try{
   await db.query("DELETE FROM sessions.owner_buffered_parity WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3",key);
   await db.query(`INSERT INTO sessions.owner_buffered_parity(org_id,workspace_id,user_id,cutover_epoch,head_epoch,head_version,head_digest,head_kind,no_head,privacy_baseline_sha256,deletion_set_digest,reset_set_digest,target_lineage_digest,target_generation)
   SELECT $1,$2,$3,$4,$5::text,$6::bigint,$12::text,$14::text,$5::text IS NULL,$15::text,coalesce($8,e.deletion_set_digest),coalesce($9,e.reset_set_digest),coalesce($10,e.target_lineage_digest),coalesce($11,e.target_generation)
     FROM sessions.owner_buffered_parity_expected($1,$2,$3,$4,$6::bigint,$7,$13::text) AS e`,
    [...key,h.cutover,h.epoch,h.version,h.digest,o.deletion??null,o.reset??null,o.lineage??null,o.generation??null,o.rowHeadDigest??h.digest,h.kind,o.rowHeadKind!==undefined?o.rowHeadKind:h.kind,o.baseline??h.baseline??baseline]);
   await db.exec("COMMIT");
  }catch(e){await db.exec("ROLLBACK");throw e}
 };
 /** Control receipts from..to at epoch for the caller (owner insert); version `to` carries tip, others a synthetic digest. */
 const receipts=async(to:number,tip=D3,o:{from?:number;skip?:number[];epoch?:string;user?:string;kind?:string}={})=>{
  for(let v=o.from??1;v<=to;v++){if(o.skip?.includes(v))continue;
   await db.query("INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation) VALUES($1,$2,$3,$4,$5,$6,$8,'{}',$7)",[TENANT.orgId,TENANT.workspaceId,o.user??TENANT.userId,o.epoch??E1,v,v===to?tip:v.toString(16).padStart(64,"0"),generation,v===to?(o.kind??"selection"):"selection"]);}
 };
 /** Set the immutable admission version of every current claim (and its ACK decisions) of the caller. */
 const admit=async(version:string|null,where="TRUE")=>{
  await db.query("UPDATE sessions.ingest_claim_attempts SET source_control_version=$1::bigint WHERE user_id=$2 AND "+where,[version,TENANT.userId]);
  if(version!==null)await db.query("UPDATE sessions.ingest_lazy_ack_decisions d SET source_control_version=$1::bigint FROM sessions.ingest_claim_attempts a WHERE a.claim_token=d.claim_token AND a.user_id=$2 AND "+where.replace(/\bbatch_id\b/g,"a.batch_id"),[version,TENANT.userId]);
 };
 return {db,upload,asReader,gate,verdict,claimOf,hit,prove,receipts,admit};
}
type Hit={claim_token:string;source:string;session_id:string};
type Tenant={orgId:string;workspaceId:string;userId:string};
type Head={cutover:string|null;epoch:string|null;version:string|null;digest:string|null;kind:string|null;pending:boolean|null;baseline:string|null};
/** The R2 answer with no head for the caller at the active cutover epoch (the wire's version "0", null epoch/digest/kind). */
const NONE:Head={cutover:E1,epoch:null,version:null,digest:null,kind:null,pending:false,baseline:"a".repeat(64)};
/** A completed head (cutover epoch, version, digest, kind selection). */
const at=(version:number|string,digest=D3,o:Partial<Head>={}):Head=>({cutover:E1,epoch:E1,version:String(version),digest,kind:"selection",pending:false,baseline:"a".repeat(64),...o});
const S=(n:number)=>`7b0f5a8e-1c2d-4e3f-8a9b-0c1d2e3f4a${String(n).padStart(2,"0")}`;


/** A healthy owner: S(1) admitted at (E1,7), contiguous receipts 1..7, parity proven at head 7. */
async function healthy(){const f=await fixture();await f.upload(S(1));await f.upload(S(2));await f.admit("7");await f.receipts(7);await f.prove(at(7));return f}
async function code(p:Promise<unknown>){const e=await p.then(()=>null,(e)=>e) as any;return e?.code as string|undefined}
const DELETE_SQL="INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude',$5)";
const FROZEN_SQL="INSERT INTO sessions.ingest_frozen_source_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude',$5)";

test("the Worker's head encoding is 043700's: a parsed no-head and a v7 head map to the reader's parameters",()=>{
 const owner={orgId:TENANT.orgId,workspaceId:TENANT.workspaceId,userId:TENANT.userId};
 const wire=(o:Record<string,unknown>)=>parseControlHead({protocol:"relay-session-read-control-head-v1",version:1,issuer:"relayhistory-cloud",tenant:owner,controlEpoch:E1,cutover:{epoch:E1,status:"active",mode:"ingest"},privacyBaselineSha256:baseline,pending:false,...o},owner)!;
 expect(gateHeadParams(wire({sourceControlEpoch:null,sourceControlVersion:"0",sourceControlDigest:null,sourceControlKind:null}))).toEqual(hp(NONE));
 expect(gateHeadParams(wire({sourceControlEpoch:E1,sourceControlVersion:"7",sourceControlDigest:D3,sourceControlKind:"selection"}))).toEqual(hp(at(7)));
});
const hp=(h:Head)=>[h.cutover,h.epoch,h.version,h.digest,h.kind,h.pending,h.baseline];

test("positive: a healthy head-7 owner's hit is visible; a no-head owner admitted at 0 is visible",async()=>{
 const f=await healthy();
 const rows=(await f.gate([await f.hit(S(1)),await f.hit(S(2)),await f.hit(S(1))],at(7))).sort((a,b)=>a.session_id<b.session_id?-1:1);
 expect(rows).toEqual([
  {claim_token:await f.claimOf(S(1)),source:"claude",session_id:S(1),drop_reason:null},
  {claim_token:await f.claimOf(S(2)),source:"claude",session_id:S(2),drop_reason:null},
 ]);
 const g=await fixture();await g.upload(S(1));await g.prove();
 expect(await g.verdict(await g.hit(S(1)),NONE)).toBeNull();
});

test("ACL: bulls96_read_api cannot read any gate table directly (42501), yet the function call succeeds",async()=>{
 const f=await healthy();
 for(const t of ["owner_buffered_parity","ingest_claim_attempts","ingest_lazy_policy","ingest_lazy_ack_decisions","upload_deletions","upload_resets","ingest_frozen_source_deletions","ingest_control_receipts","ingest_privacy_authority","ingest_batches","ingest_fence_versions"])
  expect([t,await code(f.asReader("SELECT count(*) FROM sessions."+t))]).toEqual([t,"42501"]);
 expect(await code(f.asReader("SELECT * FROM sessions.owner_buffered_parity_expected($1,$2,$3,$4,7,$5,'selection')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,E1,D3]))).toBe("42501");
 expect(await f.verdict(await f.hit(S(1)),at(7))).toBeNull();
 // EXECUTE is the reader role's only, and SECURITY DEFINER with a pinned search_path.
 const p=(await f.db.query("SELECT p.prosecdef,p.provolatile,p.proconfig,array(SELECT r.rolname FROM aclexplode(p.proacl) a JOIN pg_roles r ON r.oid=a.grantee WHERE a.privilege_type='EXECUTE' AND a.grantee<>p.proowner ORDER BY 1) AS grantees FROM pg_proc p WHERE p.oid='sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)'::regprocedure")).rows[0] as any;
 expect(p).toEqual({prosecdef:true,provolatile:"s",proconfig:["search_path=pg_catalog, sessions, pg_temp"],grantees:["bulls96_read_api"]});
});

test("caller and pin refusals: other roles, a later stray grant, missing or mismatched pins are 42501",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 for(const role of ["bulls96_session_ingest","bulls96_session_ingest_seeder","anon","authenticated","service_role"])
  expect([role,await code(f.gate([h],at(7),{role}))]).toEqual([role,"42501"]);
 // A grant added after creation is still refused at call time by the role guard.
 await f.db.exec("GRANT EXECUTE ON FUNCTION sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb) TO bulls96_session_ingest");
 const e=await f.gate([h],at(7),{role:"bulls96_session_ingest"}).then(()=>null,(e)=>e) as any;
 expect([e?.code,e?.message]).toEqual(["42501","buffered search gate: caller is not the read api role"]);
 for(const o of [{bind:{org:null}},{bind:{ws:null}},{bind:{org:""}},{ws:"ws-b"},{org:"org-b"},{bind:{ws:"ws-b"}}])
  expect([o,await code(f.gate([h],at(7),o))]).toEqual([o,"42501"]);
});

test("caller identity: the genuine bulls96_read_api login passes; SET ROLE into it by a member is 42501 and reveals no gate status",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 // Genuine login: session_user is the read role and no SET ROLE is in effect (role GUC 'none').
 expect(await f.asReader("SELECT session_user::text AS s,current_setting('role') AS r")).toEqual([{s:"bulls96_read_api",r:"none"}]);
 expect(await f.gate([h],at(7))).toEqual([{claim_token:h.claim_token,source:"claude",session_id:S(1),drop_reason:null}]);
 // A gate-closed owner is visible as such to the genuine login only.
 const closed=at(7,D3,{pending:true});
 expect(await f.verdict(h,closed)).toBe("gate_closed");
 // A LOGIN granted bulls96_read_api (the membership the bootstrap and verifier now refuse) assumes it.
 await f.db.exec("CREATE ROLE gate_probe_member LOGIN NOINHERIT;GRANT bulls96_read_api TO gate_probe_member");
 expect(await f.asReader("SELECT session_user::text AS s,current_user::text AS c,current_setting('role') AS r",[],{},"bulls96_read_api","gate_probe_member"))
  .toEqual([{s:"gate_probe_member",c:"bulls96_read_api",r:"bulls96_read_api"}]);
 for(const head of [at(7),closed,NONE]){
  const e=await f.gate([h],head,{via:"gate_probe_member"}).then((rows)=>({rows}),(e)=>e) as any;
  expect([e?.code,e?.message,e?.rows]).toEqual(["42501","buffered search gate: caller is not the read api role",undefined]);
 }
 // The read login itself running a (no-op) SET ROLE to its own name is not the genuine-login shape either.
 const self=await f.gate([h],at(7),{via:"bulls96_read_api"}).then(()=>null,(e)=>e) as any;
 expect([self?.code,self?.message]).toEqual(["42501","buffered search gate: caller is not the read api role"]);
 // A superuser session that SETs ROLE to bulls96_read_api is refused too (session_user is not the read login).
 const su=await f.gate([h],at(7),{via:"postgres"}).then(()=>null,(e)=>e) as any;
 expect(su?.code).toBe("42501");
});

test("verifier: a member of bulls96_read_api (anyone who could SET ROLE into it) fails session_read_api_role_verify.sql",async()=>{
 const f=await fixture();
 const VERIFY=psql("supabase/bootstrap/session_read_api_role_verify.sql");
 await f.db.exec(VERIFY);
 await f.db.exec("CREATE ROLE verify_probe_member LOGIN NOINHERIT;GRANT bulls96_read_api TO verify_probe_member");
 const e=await f.db.exec(VERIFY).then(()=>null,(e)=>e) as any;
 expect(e?.code).toBe("P0001");
 expect(e?.message).toContain("granted to verify_probe_member");
 // Even a non-assumable ADMIN-only edge is refused outside hoopdb.db_mode=supabase (it must also be
 // the frozen Supabase creator edge: member postgres, grantor supabase_admin).
 await f.db.exec("REVOKE bulls96_read_api FROM verify_probe_member;GRANT bulls96_read_api TO verify_probe_member WITH ADMIN TRUE, INHERIT FALSE, SET FALSE");
 expect(await code(f.db.exec(VERIFY))).toBe("P0001");
 await f.db.exec("BEGIN;SELECT set_config('hoopdb.db_mode','supabase',true)");
 expect(await code(f.db.exec(VERIFY))).toBe("P0001");
 await f.db.exec("ROLLBACK");
 await f.db.exec("REVOKE bulls96_read_api FROM verify_probe_member");
 await f.db.exec(VERIFY);
 // The bare-role bootstrap refuses an inbound member as well (55000), changing nothing.
 await f.db.exec("GRANT bulls96_read_api TO verify_probe_member");
 const BOOTSTRAP=readFileSync(join(root,"supabase/bootstrap/session_read_api_roles.sql"),"utf8");
 expect(await code(f.db.exec(BOOTSTRAP))).toBe("55000");
 await f.db.exec("REVOKE bulls96_read_api FROM verify_probe_member");
 // The one tolerated edge, Supabase PG16+'s creator edge (member postgres, grantor the superuser
 // supabase_admin, ADMIN without INHERIT or SET, hoopdb.db_mode=supabase), needs a non-bootstrap
 // CREATEROLE postgres that PGlite cannot model; it is the predicate of acquisition_installer_verify.sql.
 await f.db.exec(VERIFY);
 await f.db.exec(BOOTSTRAP);
});

test("malformed input returns no rows (the Worker drops every hit), never an error",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 const bad:[string,unknown,Head][]=[
  ["501 hits",Array.from({length:501},()=>h),at(7)],["no hits",[],at(7)],["not an array",h,at(7)],
  ["extra key",[{...h,user_id:TENANT.userId}],at(7)],["missing key",[{claim_token:h.claim_token,source:"claude"}],at(7)],
  ["uppercase token",[{...h,claim_token:h.claim_token.toUpperCase()}],at(7)],["empty session",[{...h,session_id:""}],at(7)],
  ["numeric source",[{...h,source:1}],at(7)],
  ["version 0 as a head",[h],at(0)],["leading-zero version",[h],at("07")],["version above bigint",[h],at("9223372036854775808")],
  ["no-head pending",[h],{...NONE,pending:true}],["NULL pending",[h],{...at(7),pending:null}],["partial head",[h],{...at(7),digest:null}],
  ["bad kind",[h],at(7,D3,{kind:"erase"})],["bad baseline",[h],{...at(7),baseline:"A".repeat(64)}],["NULL cutover",[h],{...at(7),cutover:null}],
 ];
 for(const [name,hits,head] of bad)expect([name,await f.gate(hits,head)]).toEqual([name,[]]);
 expect(await f.gate([h],at(7),{owner:""})).toEqual([]);
 // 500 is accepted.
 expect((await f.gate(Array.from({length:500},()=>h),at(7))).length).toBe(1);
});

test("P0-1 stale head: parity proven at v7, control now v8: dropped under the v8 head and under the v7 head",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 await f.receipts(8,D2,{from:8});
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
 await f.admit("8");
 expect(await f.verdict(h,at(8,D2))).toBe("parity_stale");
 await f.prove(at(8,D2));
 expect(await f.verdict(h,at(8,D2))).toBeNull();
 // Same version, other digest or kind than the row: stale.
 expect(await f.verdict(h,at(8,D9))).toBe("parity_stale");
 expect(await f.verdict(h,at(8,D2,{kind:"reset"}))).toBe("parity_stale");
});

test("P0-1 stale baseline: a parity row on another baseline, or a head baseline other than the policy's, drops",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 await f.prove(at(7),{baseline:"b".repeat(64)});
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
 await f.prove(at(7));
 expect(await f.verdict(h,{...at(7),baseline:"b".repeat(64)})).toBe("gate_closed");
 expect(await f.verdict(h,at(7))).toBeNull();
});

test("P0-1 stale generation: the role validation moved on (or failed) after the proof: dropped",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 await f.db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1",[G8]);
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
 await f.db.query("UPDATE sessions.__96bulls_role_validation SET generation=$1,status='fail'",[generation]);
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
 await f.db.query("UPDATE sessions.__96bulls_role_validation SET status='pass'");
 expect(await f.verdict(h,at(7))).toBeNull();
 await f.prove(at(7),{generation:G8});
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
});

test("P0-1 stale deletion digest: a deletion of ANOTHER session since the proof drops this hit too",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 await f.db.query(DELETE_SQL,[TENANT.orgId,TENANT.workspaceId,TENANT.userId,TURN_ORIGIN_ID,S(2)]);
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
 await f.prove(at(7));
 expect(await f.verdict(h,at(7))).toBeNull();
 await f.prove(at(7),{deletion:D9});
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
});

test("P0-1 stale reset digest: a reset since the proof drops; reproved it still drops as reset",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 await f.db.query("INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id) VALUES($1,$2,$3,'r-1')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
 await f.prove(at(7));
 expect(await f.verdict(h,at(7))).toBe("reset");
 await f.prove(at(7),{reset:D9});
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
});

test("P0-1 stale lineage or receipts: a planted lineage digest, a receipt gap, or a tip digest mismatch drops",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 await f.prove(at(7),{lineage:D9});
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
 await f.prove(at(7));
 expect(await f.verdict(h,at(7))).toBeNull();
 // Receipts no longer prove the head (tip digest mismatch): receipts_ok false.
 await f.db.query("UPDATE sessions.ingest_control_receipts SET digest=$1 WHERE version=7",[D2]);
 expect(await f.verdict(h,at(7))).toBe("parity_stale");
 const g=await fixture();await g.upload(S(1));await g.admit("7");await g.receipts(7,D3,{skip:[3]});await g.prove(at(7));
 expect(await g.verdict(await g.hit(S(1)),at(7))).toBe("parity_stale");
});

test("gate closed: pending head, head epoch other than the cutover, fenced authority, expired policy; parity row missing",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 expect(await f.verdict(h,{...at(7),pending:true})).toBe("gate_closed");
 expect(await f.verdict(h,at(7,D3,{epoch:E2}))).toBe("gate_closed");
 expect(await f.verdict(h,at(7,D3,{cutover:E2}))).toBe("admission_epoch_mismatch");
 await f.db.query("DELETE FROM sessions.owner_buffered_parity");
 expect(await f.verdict(h,at(7))).toBe("parity_missing");
 await f.prove(at(7));
 expect(await f.verdict(h,at(7))).toBeNull();
 await f.db.query("UPDATE sessions.ingest_lazy_policy SET valid_until=now()-interval '1 second'");
 expect(await f.verdict(h,at(7))).toBe("gate_closed");
 const g=await healthy();
 await g.db.query("INSERT INTO sessions.ingest_privacy_authority(singleton,state,control_epoch) VALUES(true,'fenced',$1)",[epoch]);
 expect(await g.verdict(await g.hit(S(1)),at(7))).toBe("gate_closed");
});

test("P0-2 deletions are not origin-scoped: an upload_deletions entry under another origin drops the hit",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 expect((await f.db.query("SELECT DISTINCT origin_id FROM sessions.ingest_claim_attempts")).rows).toEqual([{origin_id:TURN_ORIGIN_ID}]);
 await f.db.query(DELETE_SQL,[TENANT.orgId,TENANT.workspaceId,TENANT.userId,"some-other-origin",S(1)]);
 await f.prove(at(7));
 expect(await f.verdict(h,at(7))).toBe("deleted");
 expect(await f.verdict(await f.hit(S(2)),at(7))).toBeNull();
});

test("P0-2 frozen source deletions are not origin-scoped: an entry under another origin drops the hit",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 await f.db.query(FROZEN_SQL,[TENANT.orgId,TENANT.workspaceId,TENANT.userId,"some-other-origin",S(1)]);
 await f.prove(at(7));
 expect(await f.verdict(h,at(7))).toBe("deleted");
 // Another user's deletion of the same session does not hide this owner's hit.
 const g=await healthy();
 await g.db.query(FROZEN_SQL,[TENANT.orgId,TENANT.workspaceId,"user-2",TURN_ORIGIN_ID,S(1)]);
 await g.prove(at(7));
 expect(await g.verdict(await g.hit(S(1)),at(7))).toBeNull();
});

test("P1 binding: claim T for session B carried by a hit on session A (same user) is dropped",async()=>{
 const f=await healthy();
 const tA=await f.claimOf(S(1)),tB=await f.claimOf(S(2));
 expect(tA).not.toBe(tB);
 expect(await f.verdict({claim_token:tB,source:"claude",session_id:S(1)},at(7))).toBe("admission_session_mismatch");
 expect(await f.verdict({claim_token:tA,source:"codex",session_id:S(1)},at(7))).toBe("admission_session_mismatch");
 expect(await f.verdict({claim_token:tA,source:"claude",session_id:S(1)},at(7))).toBeNull();
});

test("P1 binding fails closed when the claim's records are retired",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET records_sha256=encode(sha256(convert_to(records::text,'UTF8')),'hex'),records_count=jsonb_array_length(records),records_archive_key='archive/k',records_retired_at=now(),records=NULL WHERE claim_token=$1::uuid",[h.claim_token]);
 expect(await f.verdict(h,at(7))).toBe("admission_session_mismatch");
});

test("admission: another user's or tenant's claim yields no row; a version or epoch mismatch drops",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 expect(await f.gate([h],at(7),{owner:"user-2"})).toEqual([]);
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET source_control_version=5 WHERE claim_token=$1::uuid",[h.claim_token]);
 await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET source_control_version=5 WHERE claim_token=$1::uuid",[h.claim_token]);
 expect(await f.verdict(h,at(7))).toBe("admission_version_mismatch");
 await f.admit("7");
 await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET source_control_version=6 WHERE claim_token=$1::uuid",[h.claim_token]);
 expect(await f.verdict(h,at(7))).toBe("admission_version_mismatch");
 await f.admit("7");
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET source_identity_sha256=$1 WHERE claim_token=$2::uuid",["f".repeat(64),h.claim_token]);
 expect(await f.verdict(h,at(7))).toBe("admission_epoch_mismatch");
});

test("read-only: the reader takes no row or advisory lock",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 const rows=await f.asReader("SELECT (SELECT count(*) FROM sessions.buffered_search_gate_for($1,$2,$3,$4,$5,$6,$7,$8,$9::boolean,$10,$11::jsonb)) AS n,(SELECT count(*) FROM pg_locks WHERE locktype IN ('advisory','tuple','transactionid') AND pid=pg_backend_pid()) AS locks",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,...hp(at(7)),JSON.stringify([h])]);
 expect(rows.map((r)=>[Number(r.n),Number(r.locks)])).toEqual([[1,0]]);
 for(const file of ["20261105043860_buffered_search_gate.sql","20261105043890_buffered_search_gate_retired_witness.sql"]){
  const def=readFileSync(join(root,"supabase/migrations",file),"utf8");
  expect(def).not.toMatch(/FOR (UPDATE|SHARE)|pg_advisory|\b(INSERT|UPDATE|DELETE)\s+(INTO\s+)?sessions\./u);
 }
});

/** Owner-side fixture surgery with triggers and FK actions off (replica), so a test can plant a row state the
 *  production writers never leave (CHECK constraints still apply). */
async function raw(f:{db:PGlite},sql:string,params:unknown[]=[]){
 await f.db.exec("BEGIN;SET LOCAL session_replication_role=replica");
 try{await f.db.query(sql,params as never);await f.db.exec("COMMIT")}catch(e){await f.db.exec("ROLLBACK");throw e}
}
const BATCH_OF="(org_id,workspace_id,origin_id,batch_id)=(SELECT org_id,workspace_id,origin_id,batch_id FROM sessions.ingest_claim_attempts WHERE claim_token=$1::uuid)";

/** 043860's exact gate body (the 043890 rollback's), for the causal check below. */
const GATE_043860=(()=>{const src=readFileSync(join(root,"supabase/migrations/20261105043860_buffered_search_gate.sql"),"utf8");
 return src.match(/CREATE FUNCTION sessions\.buffered_search_gate_for\([\s\S]*?\n\$gate\$;/)![0].replace("CREATE FUNCTION","CREATE OR REPLACE FUNCTION")})();

test("043890 retired witness: a claim the REAL 043880 Phase A retires stays hidden (retired), not re-exposed by its deleted ACK decisions",async()=>{
 const f=await healthy();const h=await f.hit(S(1)),control=await f.hit(S(2));
 // Before the cleanup: one lazy ACK decision of S(1)'s claim disagrees with the claim's version.
 await f.db.query("UPDATE sessions.ingest_lazy_ack_decisions SET source_control_version=6 WHERE claim_token=$1::uuid",[h.claim_token]);
 const decisions=async(t:string)=>Number(((await f.db.query("SELECT count(*) AS n FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=$1::uuid",[t])).rows[0] as any).n);
 const tombstoned=async(t:string)=>((await f.db.query("SELECT EXISTS(SELECT 1 FROM sessions.ingest_bookkeeping_retired_claims WHERE claim_token=$1::uuid) AS x",[t])).rows[0] as any).x as boolean;
 const before=await decisions(h.claim_token),controlBefore=await decisions(control.claim_token);
 expect([before>0,controlBefore>0]).toEqual([true,true]);
 expect(await f.verdict(h,at(7))).toBe("admission_version_mismatch");
 expect(await f.verdict(control,at(7))).toBeNull();
 // Make ONLY S(1)'s claim terminal for Phase A: its batch committed 2 days ago (min age 1 day), its stage
 // intents redacted, no Babysitter evidence. S(2)'s batch stays young, so Phase A blocks it.
 await raw(f,"UPDATE sessions.ingest_batches SET committed_at=clock_timestamp()-interval '2 days' WHERE "+BATCH_OF,[h.claim_token]);
 await raw(f,"UPDATE sessions.ingest_stage_intents SET cleanup_state='redacted' WHERE claim_token=$1::uuid",[h.claim_token]);
 // The operator's Phase A index (scripts/ops/ingest-bookkeeping-cleanup/phase_a_index.sql, without CONCURRENTLY) and
 // the owner switch, then one REAL execute page of sessions.ingest_bookkeeping_cleanup_decisions as the owner.
 await f.db.exec("CREATE INDEX babysitter_evidence_index_claim_idx ON sessions.babysitter_evidence_index(claim_token)");
 await f.db.query("SELECT sessions.ingest_bookkeeping_cleanup_configure($1::jsonb)",[JSON.stringify({enabled:true,dryRun:false,decisions:true,minAgeDays:1})]);
 await f.db.exec("BEGIN;SET LOCAL statement_timeout=5000");
 let page:any;
 try{page=((await f.db.query("SELECT sessions.ingest_bookkeeping_cleanup_decisions($1::jsonb) AS r",[JSON.stringify({after:null,rowBudget:500,execute:true})])).rows[0] as any).r;await f.db.exec("COMMIT")}
 catch(e){await f.db.exec("ROLLBACK");throw e}
 expect([page.mode,page.terminalClaims,page.blocked.uncommittedOrYoung,page.skipped,page.deleted]).toEqual(["execute",1,1,0,before]);
 expect([await tombstoned(h.claim_token),await decisions(h.claim_token)]).toEqual([true,0]);
 expect([await tombstoned(control.claim_token),await decisions(control.claim_token)]).toEqual([false,controlBefore]);
 // Re-prove the owner's parity row, in case the batch/stage surgery moved a parity input.
 await f.prove(at(7));
 // After the cleanup the same hit is hidden as retired; the untouched control claim's verdict is unchanged.
 expect(await f.verdict(h,at(7))).toBe("retired");
 expect(await f.verdict(control,at(7))).toBeNull();
 // Causal: the 043860 body (no retired witness) would now SHOW the hit, because the disagreeing decision is gone.
 await f.db.exec(GATE_043860);
 expect(await f.verdict(h,at(7))).toBeNull();
 expect(await f.verdict(control,at(7))).toBeNull();
 await f.db.exec(readFileSync(join(root,"supabase/migrations/20261105043890_buffered_search_gate_retired_witness.sql"),"utf8"));
 expect(await f.verdict(h,at(7))).toBe("retired");
 // retired sits after the epoch witness and before every later reason: an epoch mismatch still wins.
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET source_identity_sha256=$1 WHERE claim_token=$2::uuid",["f".repeat(64),h.claim_token]);
 expect(await f.verdict(h,at(7))).toBe("admission_epoch_mismatch");
 const closed=at(7,D3,{pending:true});
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET source_identity_sha256=$1 WHERE claim_token=$2::uuid",[identity,h.claim_token]);
 expect([await f.verdict(h,closed),await f.verdict(control,closed)]).toEqual(["retired","gate_closed"]);
});

test("043890 keeps 043860's ACL: SECURITY DEFINER, STABLE, pinned search_path, EXECUTE for bulls96_read_api only; the down is refused once a claim is retired",async()=>{
 const f=await healthy();
 const p=(await f.db.query("SELECT p.prosecdef,p.provolatile,p.proconfig,p.prosrc LIKE '%ingest_bookkeeping_retired_claims%' AS witness,array(SELECT r.rolname FROM aclexplode(p.proacl) a JOIN pg_roles r ON r.oid=a.grantee WHERE a.privilege_type='EXECUTE' AND a.grantee<>p.proowner ORDER BY 1) AS grantees FROM pg_proc p WHERE p.oid='sessions.buffered_search_gate_for(text,text,text,text,text,text,text,text,boolean,text,jsonb)'::regprocedure")).rows[0] as any;
 expect(p).toEqual({prosecdef:true,provolatile:"s",proconfig:["search_path=pg_catalog, sessions, pg_temp"],witness:true,grantees:["bulls96_read_api"]});
 // bulls96_read_api still cannot read the tombstone table directly.
 expect(await code(f.asReader("SELECT count(*) FROM sessions.ingest_bookkeeping_retired_claims"))).toBe("42501");
 const down=readFileSync(join(root,"supabase/rollbacks/20261105043890_buffered_search_gate_retired_witness.down.sql"),"utf8");
 // With a tombstone (owner insert), the down refuses and changes nothing.
 await f.db.query("INSERT INTO sessions.ingest_bookkeeping_retired_claims(claim_token) VALUES($1::uuid)",[(await f.hit(S(1))).claim_token]);
 const e=await f.db.exec("BEGIN;"+down+"COMMIT;").then(()=>null,(e)=>e) as any;
 await f.db.exec("ROLLBACK").catch(()=>{});
 expect([e?.code,e?.message]).toEqual(["55000","buffered search gate retired witness: claims were retired; turn the gate off instead of rolling back"]);
 expect(await f.verdict(await f.hit(S(1)),at(7))).toBe("retired");
});

test("043700 batch predicate: not committed, not buffered, committed_at NULL, other digest/user or a missing batch row drops",async()=>{
 const cases:[string,string][]=[
  ["publication_state canonical","UPDATE sessions.ingest_batches SET publication_state='canonical' WHERE "+BATCH_OF],
  ["state pending","UPDATE sessions.ingest_batches SET state='pending',committed_at=NULL WHERE "+BATCH_OF],
  ["committed_at NULL","ALTER TABLE sessions.ingest_batches DROP CONSTRAINT ingest_batches_check;UPDATE sessions.ingest_batches SET committed_at=NULL WHERE "+BATCH_OF],
  ["other digest","UPDATE sessions.ingest_batches SET digest=repeat('9',64) WHERE "+BATCH_OF],
  ["other user","UPDATE sessions.ingest_batches SET user_id='user-2' WHERE "+BATCH_OF],
  ["missing batch row","DELETE FROM sessions.ingest_batches WHERE "+BATCH_OF],
 ];
 for(const [name,sql] of cases){
  const f=await healthy();const h=await f.hit(S(1));
  if(name==="committed_at NULL"){const [ddl,dml]=sql.split(";");await f.db.exec(ddl!);await raw(f,dml!,[h.claim_token])}else await raw(f,sql,[h.claim_token]);
  await f.prove(at(7));
  expect([name,await f.verdict(h,at(7)),await f.verdict(await f.hit(S(2)),at(7))]).toEqual([name,"batch_not_buffered",null]);
 }
});

test("RULE 5(c): a reset at or after the batch's acceptance drops even when every reset epoch is current; one before passes",async()=>{
 const f=await healthy();const h=await f.hit(S(1)),h2=await f.hit(S(2));
 await f.db.query("INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id) VALUES($1,$2,$3,'r-1')",[TENANT.orgId,TENANT.workspaceId,TENANT.userId]);
 // Align the claims' admission reset epoch and every epochs entry with the new reset count (1), so only the time check remains.
 await raw(f,"UPDATE sessions.ingest_claim_attempts SET reset_epoch=1,epochs=(SELECT jsonb_agg(e||jsonb_build_object('reset_epoch',1)) FROM jsonb_array_elements(epochs) e) WHERE user_id=$1",[TENANT.userId]);
 await f.prove(at(7));
 const acceptedAt=async(t:string)=>((await f.db.query("SELECT committed_at::text c FROM sessions.ingest_batches WHERE "+BATCH_OF,[t])).rows[0] as any).c as string;
 // reset_at (now) >= committed_at: dropped as reset.
 expect([await f.verdict(h,at(7)),await f.verdict(h2,at(7))]).toEqual(["reset","reset"]);
 // reset_at == committed_at of S(1)'s batch: still dropped (>=).
 await raw(f,"UPDATE sessions.upload_resets SET reset_at=$1::timestamptz WHERE request_id='r-1'",[await acceptedAt(h.claim_token)]);
 await f.prove(at(7));
 expect(await f.verdict(h,at(7))).toBe("reset");
 // reset_at < committed_at of both batches: the fully consistent hits pass.
 await raw(f,"UPDATE sessions.upload_resets SET reset_at=$1::timestamptz-interval '1 second' WHERE request_id='r-1'",[await acceptedAt(h.claim_token)]);
 await f.prove(at(7));
 expect([await f.verdict(h,at(7)),await f.verdict(h2,at(7))]).toEqual([null,null]);
 // A per-session epochs entry still on the old reset epoch drops as reset.
 await raw(f,"UPDATE sessions.ingest_claim_attempts SET epochs=(SELECT jsonb_agg(e||jsonb_build_object('reset_epoch',0)) FROM jsonb_array_elements(epochs) e) WHERE claim_token=$1::uuid",[h.claim_token]);
 expect([await f.verdict(h,at(7)),await f.verdict(h2,at(7))]).toEqual(["reset",null]);
});

test("supersession: a fence bump after admission, a missing or an ambiguous epochs entry drops as superseded",async()=>{
 const f=await healthy();const h=await f.hit(S(1)),h2=await f.hit(S(2));
 const entry=((await f.db.query("SELECT e FROM sessions.ingest_claim_attempts,jsonb_array_elements(epochs) e WHERE claim_token=$1::uuid AND e->>'session_id'=$2",[h.claim_token,S(1)])).rows as any[]);
 expect(entry.length).toBe(1);
 const fence=Number(entry[0].e.epoch);
 // A newer fence version (deletion/disable/reset bumped it) on the claim's origin: this claim is not the latest admission.
 await raw(f,"INSERT INTO sessions.ingest_fence_versions(org_id,workspace_id,user_id,origin_id,source,session_id,epoch) VALUES($1,$2,$3,$4,'claude',$5,$6) ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=EXCLUDED.epoch",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,TURN_ORIGIN_ID,S(1),fence+1]);
 await f.prove(at(7));
 expect([await f.verdict(h,at(7)),await f.verdict(h2,at(7))]).toEqual(["superseded",null]);
 // The same fence version on ANOTHER origin does not supersede (043700 matches the stage's origin).
 const g=await healthy();const k=await g.hit(S(1));
 await raw(g,"INSERT INTO sessions.ingest_fence_versions(org_id,workspace_id,user_id,origin_id,source,session_id,epoch) VALUES($1,$2,$3,'some-other-origin','claude',$4,$5)",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,S(1),fence+1]);
 await g.prove(at(7));
 expect(await g.verdict(k,at(7))).toBeNull();
 // Missing epochs entry for the session.
 await raw(g,"UPDATE sessions.ingest_claim_attempts SET epochs=(SELECT coalesce(jsonb_agg(e),'[]') FROM jsonb_array_elements(epochs) e WHERE e->>'session_id'<>$2) WHERE claim_token=$1::uuid",[k.claim_token,S(1)]);
 expect(await g.verdict(k,at(7))).toBe("superseded");
 // Ambiguous: two entries for the session with different epochs.
 await raw(g,"UPDATE sessions.ingest_claim_attempts SET epochs=epochs||$2::jsonb WHERE claim_token=$1::uuid",[k.claim_token,JSON.stringify([{source:"claude",session_id:S(1),epoch:fence,reset_epoch:0},{source:"claude",session_id:S(1),epoch:fence+1,reset_epoch:0}])]);
 expect(await g.verdict(k,at(7))).toBe("superseded");
 // Epochs not an array: fails closed.
 await raw(g,"ALTER TABLE sessions.ingest_claim_attempts DISABLE TRIGGER ALL");
 const c=(await g.db.query("SELECT conname FROM pg_constraint WHERE conrelid='sessions.ingest_claim_attempts'::regclass AND pg_get_constraintdef(oid) LIKE '%epochs%'")).rows as any[];
 for(const r of c)await g.db.exec(`ALTER TABLE sessions.ingest_claim_attempts DROP CONSTRAINT "${r.conname}"`);
 await raw(g,"UPDATE sessions.ingest_claim_attempts SET epochs='{}'::jsonb WHERE claim_token=$1::uuid",[k.claim_token]);
 expect(await g.verdict(k,at(7))).toBe("superseded");
});

test("a fully consistent hit still passes: committed buffered batch, reset before acceptance, current fence, one epochs entry",async()=>{
 const f=await healthy();const h=await f.hit(S(1));
 const row=(await f.db.query(`SELECT b.state,b.publication_state,b.committed_at IS NOT NULL AS accepted,b.user_id=a.user_id AND b.digest=a.digest AS same,
   a.reset_epoch,(SELECT count(*) FROM jsonb_array_elements(a.epochs) e WHERE e->>'session_id'=$2)::int AS entries
   FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_batches b USING(org_id,workspace_id,origin_id,batch_id) WHERE a.claim_token=$1::uuid`,[h.claim_token,S(1)])).rows[0];
 expect(row).toEqual({state:"committed",publication_state:"buffered",accepted:true,same:true,reset_epoch:0,entries:1} as any);
 // Bump the fence AND re-admit at the new epoch (the claim's entry carries it): still the latest admission.
 await raw(f,"INSERT INTO sessions.ingest_fence_versions(org_id,workspace_id,user_id,origin_id,source,session_id,epoch) VALUES($1,$2,$3,$4,'claude',$5,3) ON CONFLICT(org_id,workspace_id,user_id,origin_id,source,session_id) DO UPDATE SET epoch=3",[TENANT.orgId,TENANT.workspaceId,TENANT.userId,TURN_ORIGIN_ID,S(1)]);
 await raw(f,"UPDATE sessions.ingest_claim_attempts SET epochs=(SELECT jsonb_agg(CASE WHEN e->>'session_id'=$2 THEN e||'{\"epoch\":3}' ELSE e END) FROM jsonb_array_elements(epochs) e) WHERE claim_token=$1::uuid",[h.claim_token,S(1)]);
 await f.prove(at(7));
 expect(await f.verdict(h,at(7))).toBeNull();
});
