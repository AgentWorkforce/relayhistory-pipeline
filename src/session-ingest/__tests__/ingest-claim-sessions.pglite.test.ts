/**
 * 20261105044080 ingest_claim_sessions (PR 1, inert): the side table, its policy row, the writer trigger on
 * sessions.ingest_claim_attempts and the resumable backfill (src/session-ingest/claim-sessions-backfill.ts).
 * Claims are written directly as the owner (the superuser here) except in the real-upload test, which seeds
 * through POST /v1/turns/batches as bulls96_session_ingest (the real SECURITY DEFINER claim path).
 */
import {afterEach,expect,setDefaultTimeout,test} from "bun:test";
import {randomUUID,createHash} from "node:crypto";
import {PGlite} from "@electric-sql/pglite";
import {btree_gin} from "@electric-sql/pglite/contrib/btree_gin";
import {pg_trgm} from "@electric-sql/pglite/contrib/pg_trgm";
import {applySupabaseSchema} from "./pglite-schema.ts";
import {backfillClaimSessions} from "../claim-sessions-backfill.ts";
import {sqlLedger} from "../ledger-sql.ts";
import {deliveryAccount} from "../vendor/delivery/prepare.ts";
import {DELIVERY_MAPPING_VERSION} from "../vendor/delivery/delivery-contracts.ts";
import {TURN_KIND,TURN_ORIGIN_ID,turnRecordId} from "../turns.ts";
import {TENANT,scratchStore} from "./support.ts";
import {initializeFixtureSeatClock} from "./ordered-support.ts";
setDefaultTimeout(120000);

const open:Array<()=>unknown>=[];
afterEach(async()=>{for(const f of open.splice(0).reverse())await f()});
const hash=(s:string)=>createHash("sha256").update(s).digest("hex");
const T={orgId:"org-a",workspaceId:"ws-a",userId:"user-a"},origin="origin-a";
type Pair=[string,string];

async function fixture(){
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());
 await applySupabaseSchema(db,{until:null});
 // Commit-time admission prerequisites, as in claim-records-retirement.pglite.test.ts.
 const generation="a".repeat(64);
 await db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker','"+generation+"','pass') ON CONFLICT(name) DO UPDATE SET generation=excluded.generation,status='pass'");
 await db.query("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch) VALUES($1,repeat('a',64),repeat('b',64),$2,clock_timestamp()+interval '1 hour',repeat('3',64))",[T.orgId,generation]);
 await initializeFixtureSeatClock(db as never,generation);
 let serial=0;
 /** One batch (committed unless told otherwise) with one claim whose records are `records` verbatim. */
 async function claim(records:unknown,o:{committed?:boolean;token?:string}={}){
  const batch="batch-"+(++serial),token=o.token??randomUUID(),digest=hash(batch);
  await db.query("INSERT INTO sessions.ingest_batches(org_id,workspace_id,origin_id,batch_id,user_id,digest,receipt_id,state,committed_at,publication_state) VALUES($1,$2,$3,$4,$5,$6,'rhr_'||repeat('a',64),'pending',NULL,'canonical')",[T.orgId,T.workspaceId,origin,batch,T.userId,digest]);
  await db.query("INSERT INTO sessions.ingest_claim_attempts(claim_token,org_id,workspace_id,user_id,origin_id,batch_id,digest,epochs,records,reset_epoch) VALUES($1,$2,$3,$4,$5,$6,$7,'[]',$8,7)",[token,T.orgId,T.workspaceId,T.userId,origin,batch,digest,records===null?null:JSON.stringify(records)]);
  if(o.committed!==false)await db.query("UPDATE sessions.ingest_batches SET state='committed',committed_at=timestamptz '2026-11-05 10:00:00+00' WHERE batch_id=$1",[batch]);
  return {batch,token};
 }
 const rec=(source:unknown,session_id:unknown,ord=0)=>({ord,source,session_id,record_id:"r-"+ord,revision:1,digest:hash(String(ord))});
 const side=async(token?:string)=>(await db.query<{claim_token:string;source:string;session_id:string}>("SELECT claim_token,source,session_id FROM sessions.ingest_claim_sessions"+(token?" WHERE claim_token=$1":"")+" ORDER BY claim_token,source,session_id",token?[token]:[])).rows;
 const pairs=async(token:string)=>(await side(token)).map(r=>[r.source,r.session_id] as Pair);
 const count=async()=>(await db.query<{n:number}>("SELECT count(*)::int n FROM sessions.ingest_claim_sessions")).rows[0]!.n;
 const policy=async()=>(await db.query<{write_enabled:boolean;backfill_cursor:string|null;complete:boolean}>("SELECT write_enabled,backfill_cursor,backfill_complete_at IS NOT NULL AS complete FROM sessions.ingest_claim_sessions_policy")).rows;
 const enable=(on:boolean)=>db.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled=$1",[on]);
 const sql={query:(s:string,p?:unknown[])=>db.query(s,p as never) as never};
 const backfill=(o:{batchSize?:number;maxBatches?:number;dryRun?:boolean}={})=>backfillClaimSessions({sql,batchSize:o.batchSize??500,statementTimeoutMs:30000,maxBatches:o.maxBatches,dryRun:o.dryRun??false});
 /** The retirement transition exactly as the records guard (038000) accepts it. */
 const retire=(token:string)=>db.query("UPDATE sessions.ingest_claim_attempts SET records=NULL,records_sha256=encode(sha256(convert_to(records::text,'UTF8')),'hex'),records_count=jsonb_array_length(records),records_archive_key='k',records_retired_at=clock_timestamp() WHERE claim_token=$1",[token]);
 return {db,claim,rec,side,pairs,count,policy,enable,backfill,retire};
}

test("installs inert: one disabled policy row, owner-only tables and trigger functions, and a claim writes no side row",async()=>{
 const f=await fixture();
 expect(await f.policy()).toEqual([{write_enabled:false,backfill_cursor:null,complete:false}]);
 const c=await f.claim([f.rec("claude","s-1"),f.rec("codex","s-2",1)]);
 expect(await f.count()).toBe(0);
 // A records rewrite while disabled writes nothing either.
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=$2 WHERE claim_token=$1",[c.token,JSON.stringify([f.rec("claude","s-9")])]);
 expect(await f.count()).toBe(0);
 // RLS on, no policy, no grantee but the owner (as ingest_claim_attempts); trigger functions owner-only, not SECURITY DEFINER.
 const rel=(await f.db.query<{relname:string;rls:boolean;policies:number;grantees:number}>("SELECT c.relname,c.relrowsecurity rls,(SELECT count(*)::int FROM pg_policy p WHERE p.polrelid=c.oid) policies,(SELECT count(*)::int FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE a.grantee<>c.relowner) grantees FROM pg_class c WHERE c.oid IN('sessions.ingest_claim_sessions'::regclass,'sessions.ingest_claim_sessions_policy'::regclass,'sessions.ingest_claim_attempts'::regclass) ORDER BY 1")).rows;
 expect(rel).toEqual([
  {relname:"ingest_claim_attempts",rls:true,policies:0,grantees:0},
  {relname:"ingest_claim_sessions",rls:true,policies:0,grantees:0},
  {relname:"ingest_claim_sessions_policy",rls:true,policies:0,grantees:0}]);
 const fns=(await f.db.query<{proname:string;prosecdef:boolean;grantees:number}>("SELECT p.proname,p.prosecdef,(SELECT count(*)::int FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee<>p.proowner) grantees FROM pg_proc p WHERE p.proname LIKE 'ingest_claim_sessions_%' ORDER BY 1")).rows;
 expect(fns).toEqual([{proname:"ingest_claim_sessions_policy_guard",prosecdef:false,grantees:0},{proname:"ingest_claim_sessions_sync",prosecdef:false,grantees:0}]);
 for(const role of ["bulls96_session_ingest","bulls96_read_api"]){
  await f.db.exec("BEGIN;SET LOCAL ROLE "+role);
  await expect(f.db.query("SELECT 1 FROM sessions.ingest_claim_sessions")).rejects.toMatchObject({code:"42501"});
  await f.db.exec("ROLLBACK");
 }
 // Triggers: the writer on the claims; on the policy the row guard and the statement-level SHARE lock that
 // runs before the policy tuple is locked (lock order: claims table, then policy row, as a claim does).
 const trg=(await f.db.query<{t:string;rel:string;def:string}>("SELECT tgname t,tgrelid::regclass::text rel,pg_get_triggerdef(oid) def FROM pg_trigger WHERE tgname LIKE 'ingest_claim_sessions_%' ORDER BY 1")).rows.map(r=>[r.t,r.rel,r.def.replace(/^.* (BEFORE|AFTER) /,"$1 ").replace(/ ON .* FOR EACH /," FOR EACH ").replace(/ EXECUTE .*$/,"")]);
 expect(trg).toEqual([
  ["ingest_claim_sessions_policy_guard","sessions.ingest_claim_sessions_policy","BEFORE DELETE OR UPDATE FOR EACH ROW"],
  ["ingest_claim_sessions_policy_lock","sessions.ingest_claim_sessions_policy","BEFORE UPDATE OF write_enabled FOR EACH STATEMENT"],
  ["ingest_claim_sessions_sync","sessions.ingest_claim_attempts","AFTER INSERT OR UPDATE OF records FOR EACH ROW"]]);
 // The indexes PR 2 reads through.
 const idx=(await f.db.query<{d:string}>("SELECT pg_get_indexdef(indexrelid) d FROM pg_index WHERE indrelid='sessions.ingest_claim_sessions'::regclass ORDER BY 1")).rows.map(r=>r.d.replace(/^.* USING /,"")).sort();
 expect(idx).toEqual(["btree (claim_token, source, session_id)","btree (org_id, workspace_id, user_id, accepted_at DESC NULLS LAST, session_id, source)","btree (org_id, workspace_id, user_id, source, session_id)"]);
});

test("enabled: exactly the distinct (source, session_id) pairs; duplicates collapse; malformed elements are skipped, never raised",async()=>{
 const f=await fixture();
 await f.enable(true);
 const records=[
  f.rec("claude","s-1",0),f.rec("claude","s-1",1),f.rec("claude","s-1",2), // duplicates of one pair
  f.rec("codex","s-1",3),f.rec("claude","s-2",4),
  {ord:5,session_id:"s-missing-source"},{ord:6,source:"claude"}, // missing source / session_id
  f.rec("claude",null,7),f.rec(null,"s-3",8),f.rec("claude",42,9),f.rec(["x"],"s-4",10), // non-string
  "not-an-object",null,7,
 ];
 const c=await f.claim(records);
 expect(await f.pairs(c.token)).toEqual([["claude","s-1"],["claude","s-2"],["codex","s-1"]]);
 const row=(await f.db.query<any>("SELECT org_id,workspace_id,user_id,origin_id,batch_id,digest,reset_epoch::int AS reset_epoch,accepted_at FROM sessions.ingest_claim_sessions WHERE claim_token=$1 LIMIT 1",[c.token])).rows[0];
 // The claim is written before its batch commits, so the writer trigger finds no committed batch (NULL); since
 // 044090 the commit-time refresh on ingest_batches then sets accepted_at to the batch's committed_at.
 expect(row).toEqual({org_id:T.orgId,workspace_id:T.workspaceId,user_id:T.userId,origin_id:origin,batch_id:c.batch,digest:hash(c.batch),reset_epoch:7,accepted_at:new Date("2026-11-05T10:00:00Z")});
 // Empty and NULL records write nothing.
 const empty=await f.claim([]),none=await f.claim(null);
 expect(await f.pairs(empty.token)).toEqual([]);expect(await f.pairs(none.token)).toEqual([]);
 // A live records rewrite drops pairs no longer present and adds new ones.
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET records=$2 WHERE claim_token=$1",[c.token,JSON.stringify([f.rec("claude","s-2"),f.rec("claude","s-5",1)])]);
 expect(await f.pairs(c.token)).toEqual([["claude","s-2"],["claude","s-5"]]);
 // An update of another column does not fire it (UPDATE OF records only) and changes nothing.
 await f.db.query("UPDATE sessions.ingest_claim_attempts SET schema_generation=repeat('a',64) WHERE claim_token=$1",[c.token]);
 expect(await f.pairs(c.token)).toEqual([["claude","s-2"],["claude","s-5"]]);
 // Retirement (records -> NULL) removes every side row of the claim.
 await f.retire(c.token);
 expect(await f.pairs(c.token)).toEqual([]);
 // Deleting the batch cascades through the claim to its side rows.
 const d=await f.claim([f.rec("claude","s-6")]);
 expect(await f.pairs(d.token)).toEqual([["claude","s-6"]]);
 await f.db.query("DELETE FROM sessions.ingest_batches WHERE batch_id=$1",[d.batch]);
 expect(await f.pairs(d.token)).toEqual([]);
});

test("policy: never deleted; a write_enabled change clears cursor and completion; completion needs the writer on",async()=>{
 const f=await fixture();
 await expect(f.db.query("DELETE FROM sessions.ingest_claim_sessions_policy")).rejects.toMatchObject({code:"55000"});
 await expect(f.db.query("UPDATE sessions.ingest_claim_sessions_policy SET backfill_complete_at=clock_timestamp()")).rejects.toMatchObject({code:"23514"});
 await expect(f.db.query("INSERT INTO sessions.ingest_claim_sessions_policy(singleton) VALUES(false)")).rejects.toMatchObject({code:"23514"});
 await expect(f.db.query("UPDATE sessions.ingest_claim_sessions_policy SET backfill_cursor='not-a-uuid'")).rejects.toMatchObject({code:"23514"});
 const u=randomUUID();
 await f.db.query("UPDATE sessions.ingest_claim_sessions_policy SET backfill_cursor=$1",[u]);
 await f.enable(true);
 expect(await f.policy()).toEqual([{write_enabled:true,backfill_cursor:null,complete:false}]);
 await f.db.query("UPDATE sessions.ingest_claim_sessions_policy SET backfill_cursor=$1,backfill_complete_at=clock_timestamp()",[u]);
 // Same value: no transition, nothing cleared.
 await f.enable(true);
 expect(await f.policy()).toEqual([{write_enabled:true,backfill_cursor:u,complete:true}]);
 await f.enable(false);
 expect(await f.policy()).toEqual([{write_enabled:false,backfill_cursor:null,complete:false}]);
 // A missing policy row (only reachable by bypassing the guard) reads as disabled.
 await f.db.exec("ALTER TABLE sessions.ingest_claim_sessions_policy DISABLE TRIGGER ingest_claim_sessions_policy_guard;DELETE FROM sessions.ingest_claim_sessions_policy");
 await f.claim([f.rec("claude","s-1")]);
 expect(await f.count()).toBe(0);
});

test("backfill: dry-run writes nothing; batches resume from the cursor; completion only at the end with the writer on; counts only",async()=>{
 const f=await fixture();
 // Five pre-existing claims written while the writer was off (one retired, one uncommitted batch).
 const tokens=["10000000-0000-4000-8000-000000000001","20000000-0000-4000-8000-000000000002","30000000-0000-4000-8000-000000000003","40000000-0000-4000-8000-000000000004","50000000-0000-4000-8000-000000000005"];
 const cs=[];
 for(const [i,token] of tokens.entries())cs.push(await f.claim([f.rec("claude","s-"+i),f.rec("claude","s-"+i,1),f.rec("codex","s-x",2)],{token,committed:i!==3}));
 await f.retire(tokens[4]!);
 expect(await f.count()).toBe(0);
 const expected=tokens.slice(0,4).flatMap(t=>[[t,"claude"],[t,"codex"]]);

 // Writer off: a full run fills the rows but does not mark completion (exit-0 report).
 const off=await f.backfill({batchSize:2});
 expect(off).toMatchObject({mode:"execute",writeEnabled:false,claimsScanned:5,rowsInserted:8,reachedEnd:true,completionMarked:false,completionNotMarkedReason:"write_disabled"});
 expect((await f.side()).map(r=>[r.claim_token,r.source])).toEqual(expected);
 // Turning the writer on clears that cursor: the walk restarts after the writer is on.
 await f.db.query("DELETE FROM sessions.ingest_claim_sessions");
 await f.enable(true);
 expect(await f.policy()).toEqual([{write_enabled:true,backfill_cursor:null,complete:false}]);

 const dry=await f.backfill({batchSize:2,dryRun:true});
 expect(dry).toMatchObject({mode:"dry-run",claimsScanned:5,rowsMissing:8,rowsInserted:0,reachedEnd:true,completionMarked:false,completionNotMarkedReason:"dry_run"});
 expect(await f.count()).toBe(0);
 expect(await f.policy()).toEqual([{write_enabled:true,backfill_cursor:null,complete:false}]);

 // One batch of two, then stop: cursor advanced, not complete.
 const first=await f.backfill({batchSize:2,maxBatches:1});
 expect(first).toMatchObject({batches:1,claimsScanned:2,rowsInserted:4,reachedEnd:false,completionMarked:false,completionNotMarkedReason:"max_batches",resumed:false});
 expect(await f.policy()).toEqual([{write_enabled:true,backfill_cursor:tokens[1]!,complete:false}]);
 // A claim written now (writer on) behind the cursor is covered by the trigger, not the walk.
 const late=await f.claim([f.rec("claude","late")],{token:"00000000-0000-4000-8000-000000000000"});
 expect(await f.pairs(late.token)).toEqual([["claude","late"]]);
 // Resume: the remaining three claims, then the end with the writer on: completion marked.
 const rest=await f.backfill({batchSize:2});
 expect(rest).toMatchObject({resumed:true,claimsScanned:3,rowsInserted:4,reachedEnd:true,completionMarked:true,completionNotMarkedReason:null});
 expect((await f.side()).filter(r=>r.claim_token!==late.token).map(r=>[r.claim_token,r.source])).toEqual(expected);
 expect(await f.policy()).toEqual([{write_enabled:true,backfill_cursor:tokens[4]!,complete:true}]);
 // accepted_at: the committed batch's committed_at, NULL for the uncommitted one.
 const acc=(await f.db.query<{claim_token:string;a:string|null}>("SELECT DISTINCT claim_token,accepted_at::text a FROM sessions.ingest_claim_sessions WHERE claim_token<>$1 ORDER BY 1",[late.token])).rows;
 expect(acc).toEqual(tokens.slice(0,4).map((t,i)=>({claim_token:t,a:i===3?null:"2026-11-05 10:00:00+00"})));
 // Already complete: a rerun writes nothing.
 expect(await f.backfill()).toMatchObject({alreadyComplete:true,batches:0,completionMarked:false,completionNotMarkedReason:null});
 // The report carries counts and booleans only.
 for(const r of [off,dry,first,rest]){
  const text=JSON.stringify(r);
  for(const secret of [...tokens,late.token,T.orgId,T.workspaceId,T.userId,"s-0","claude"])expect(text).not.toContain(secret);
  for(const v of Object.values(r))expect(["number","boolean","string","object"]).toContain(typeof v);
 }
});

test("backfill reconciles: stale rows of a claim retired while the writer was off are deleted, and accepted_at is refreshed",async()=>{
 const f=await fixture();
 await f.enable(true);
 const a=await f.claim([f.rec("claude","s-1")]),b=await f.claim([f.rec("claude","s-2")]);
 expect(await f.count()).toBe(2);
 await f.enable(false);
 await f.retire(a.token);
 expect(await f.pairs(a.token)).toEqual([["claude","s-1"]]); // writer off: stale
 await f.enable(true);
 const r=await f.backfill();
 // rowsRefreshed 0: b's accepted_at was already set by 044090's commit-time refresh (the writer was on at commit).
 expect(r).toMatchObject({claimsScanned:2,rowsInserted:0,rowsRefreshed:0,rowsDeleted:1,completionMarked:true});
 expect(await f.pairs(a.token)).toEqual([]);
 expect(await f.pairs(b.token)).toEqual([["claude","s-2"]]);
 expect((await f.db.query<{a:string}>("SELECT accepted_at::text a FROM sessions.ingest_claim_sessions WHERE claim_token=$1",[b.token])).rows[0]!.a).toBe("2026-11-05 10:00:00+00");
 await expect(backfillClaimSessions({sql:{query:async()=>({rows:[]})},batchSize:0,statementTimeoutMs:30000,dryRun:true})).rejects.toMatchObject({code:"batch_size_invalid"});
});

test("backfill repairs accepted_at: a NULL (missed refresh) and a wrong value become the batch's committed_at; a pending batch stays NULL",async()=>{
 const f=await fixture();
 await f.enable(true);
 const x=await f.claim([f.rec("claude","s-1")]),y=await f.claim([f.rec("claude","s-2"),f.rec("codex","s-2",1)]),z=await f.claim([f.rec("claude","s-3")]);
 const p=await f.claim([f.rec("claude","s-4")],{committed:false});
 const acc=async(token:string)=>(await f.db.query<{a:string|null}>("SELECT accepted_at::text a FROM sessions.ingest_claim_sessions WHERE claim_token=$1 ORDER BY source,session_id",[token])).rows.map(r=>r.a);
 const at="2026-11-05 10:00:00+00";
 // The commit-time refresh (044090) set the committed batches; break x (missed refresh) and y (drifted).
 expect([await acc(x.token),await acc(y.token),await acc(z.token),await acc(p.token)]).toEqual([[at],[at,at],[at],[null]]);
 await f.db.query("UPDATE sessions.ingest_claim_sessions SET accepted_at=NULL WHERE claim_token=$1",[x.token]);
 await f.db.query("UPDATE sessions.ingest_claim_sessions SET accepted_at=timestamptz '2000-01-01 00:00:00+00' WHERE claim_token=$1",[y.token]);
 const r=await f.backfill({batchSize:2});
 // Three side rows repaired by ON CONFLICT DO UPDATE (x's one, y's two); z already right and p's NULL (no committed batch) untouched.
 expect(r).toMatchObject({claimsScanned:4,rowsInserted:0,rowsRefreshed:3,rowsDeleted:0,completionMarked:true});
 expect([await acc(x.token),await acc(y.token),await acc(z.token),await acc(p.token)]).toEqual([[at],[at,at],[at],[null]]);
 expect((await f.db.query<{n:number}>("SELECT count(*)::int n FROM sessions.ingest_claim_sessions s JOIN sessions.ingest_batches b USING(org_id,workspace_id,origin_id,batch_id) WHERE b.state='committed' AND s.accepted_at IS DISTINCT FROM b.committed_at")).rows[0]!.n).toBe(0);
});

test("real buffered upload as bulls96_session_ingest: writer off writes nothing, writer on writes one row per session",async()=>{
 const generation="4".repeat(64),epoch="1".repeat(64),identity="c".repeat(64),baseline="a".repeat(64);
 const db=new PGlite({extensions:{btree_gin,pg_trgm}});open.push(()=>db.close());
 await applySupabaseSchema(db,{until:null});
 await db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation(name,generation,status) VALUES('session_directory_search_worker','"+generation+"','pass') ON CONFLICT(name) DO UPDATE SET generation=excluded.generation,status='pass'");
 const owner=async(name:string,p:unknown)=>(await db.query("SELECT sessions."+name+"($1::jsonb) AS r",[JSON.stringify(p)])).rows[0] as any;
 await owner("ingest_lazy_policy_set",{baselineMode:"lazy-current",generation,sourceIdentitySha256:identity,controlEpoch:epoch,privacyBaselineSha256:baseline,retentionContractSha256:"b".repeat(64),sourceProofSha256:"d".repeat(64),validUntil:new Date(Date.now()+3600000).toISOString(),issuer:"relayhistory-cloud"});
 await owner("ingest_receipt_seat_initialize",{protocol:"delivery-acceptance-floor-v1",controlEpoch:epoch,capturedAt:"2026-11-04T00:00:00.000000Z",sourceSequences:["sessions.delivery_catalog_acceptance_seq","sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass,i)=>({sequenceRegclass,sequenceOid:String(i+1),allocatedHighWater:"9007199254740993",isCalled:true,incrementBy:"1",cycle:false})),allocatedHighWater:"9007199254740993",generation,sourceIdentitySha256:identity,sourceProofSha256:"d".repeat(64)});
 const run=async(s:string,p:unknown[]=[])=>{await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");try{const r=await db.query(s,p as never);await db.exec("COMMIT");return r as never}catch(e){await db.exec("ROLLBACK");throw e}};
 const ledger=sqlLedger(run,{acceptanceMode:"buffered",expectedGeneration:generation});
 const fs=scratchStore();open.push(fs.done);
 await db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",[identity,epoch,baseline,"e".repeat(64)]);
 const {createIngestApp}=await import("../app.ts");
 const app=createIngestApp({identify:()=>({...TENANT,scopes:["rth:sync"]}),deliver:{ledger,acceptanceMode:"buffered",target:{store:fs.store,keyRoot:"96bulls"}},log:()=>{},custodySource:"frozen-supabase"});
 let serial=0;
 async function upload(session:string,turns:number){
  const batchId="b-"+(++serial),source="claude";
  const records=Array.from({length:turns},(_,i)=>{
   const record_id=turnRecordId(TENANT.workspaceId,source,session,i),revision=1_793_000_000_000_000+serial*100+i;
   return {schema_version:1 as const,origin_id:TURN_ORIGIN_ID,record_id,revision_id:`turnrev:${record_id.slice(5)}:${revision}`,revision,kind:TURN_KIND as any,source,session_id:session,operation:"upsert" as const,
    payload:{id:`${String(serial*100+i).padStart(8,"0")}-aaaa-4bbb-8ccc-dddddddddddd`,org_id:TENANT.orgId,workspace_id:TENANT.workspaceId,source,session_id:session,turn_index:i,session_owner:"user-1",role:"user",content:`turn ${i}`,actor_name:"Ada",actor_role:"owner",metadata_json:"{}",ts:"2026-11-05T12:00:00.123Z",tagged:true}};
  });
  const b={schema_version:1 as const,origin_id:TURN_ORIGIN_ID,batch_id:batchId,job_id:"relayhistory-turns",generation:1,destination_id:"relayhistory",instance_id:"relayhistory-api",account_id:await deliveryAccount(TENANT),mapping_version:DELIVERY_MAPPING_VERSION,records};
  const r=await app.request("/v1/turns/batches",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({protocolVersion:1,batch:b})});
  if(r.status!==200)throw new Error("upload "+r.status+" "+await r.text());
  return batchId;
 }
 const sideFor=async(batch:string)=>(await db.query<{source:string;session_id:string;n:number}>("SELECT s.source,s.session_id,(SELECT jsonb_array_length(a.records) FROM sessions.ingest_claim_attempts a WHERE a.claim_token=s.claim_token)::int n FROM sessions.ingest_claim_sessions s WHERE s.batch_id=$1 ORDER BY 1,2",[batch])).rows;
 const off=await upload("sess-off",3);
 expect((await db.query<{n:number}>("SELECT count(*)::int n FROM sessions.ingest_claim_attempts WHERE batch_id=$1 AND records IS NOT NULL",[off])).rows[0]!.n).toBe(1);
 expect(await sideFor(off)).toEqual([]);
 await db.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled=true");
 const on=await upload("sess-on",3);
 // Three records of one session in the claim, one side row.
 expect(await sideFor(on)).toEqual([{source:"claude",session_id:"sess-on",n:3}]);
 // The backfill then picks up the claim written while the writer was off.
 const r=await backfillClaimSessions({sql:{query:(s,p)=>db.query(s,p as never) as never},batchSize:500,statementTimeoutMs:30000,dryRun:false});
 expect(r).toMatchObject({rowsInserted:1,completionMarked:true});
 expect(await sideFor(off)).toEqual([{source:"claude",session_id:"sess-off",n:3}]);
});
