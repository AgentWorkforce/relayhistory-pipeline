// Migration 20261105032000 (large-read routing fixes, #184 findings 2 and 5): per-scope discovery
// isolation with bounded retries, per-job claim isolation, measured-size-only routing, unchanged
// function boundary, apply order after 031000, and down/reapply. PGlite; no service credential.
import {afterAll,afterEach,beforeAll,expect,test} from "bun:test";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {runMigrations} from "../migrate.ts";
import {projectSessionDirectory} from "../session-directory-projector.ts";
import {clonedReplayDatabase,warmReplayTemplate,closeReplayTemplate,PGLITE_TEST_TIMEOUT,addReplayPointer,REPLAY_FIXTURE_IDENTITY as i} from "../../session-pipeline/__tests__/replay-fixture.ts";

const root=join(import.meta.dir,"../../..");
const FILE="20261105032000_session_large_read_routing_fixes";
const LEDGER=`upstream:${FILE}.sql`;
const FNS=["sessions.route_session_large_read(text,text,text,text)","sessions.discover_session_large_reads()","sessions.claim_session_large_read(uuid,integer)"] as const;
const LARGE=67108865,N1="11111111-1111-4111-8111-111111111111";
type X=Awaited<ReturnType<typeof clonedReplayDatabase>>;
const open:X[]=[];
beforeAll(()=>warmReplayTemplate(),120_000);
afterAll(()=>closeReplayTemplate());
afterEach(async()=>{for(const x of open.splice(0))await x.pg.close()});
async function fresh(){const x=await clonedReplayDatabase();open.push(x);return x}
const ids=(id:string)=>[i.orgId,i.workspaceId,i.source,id];
async function addScope(x:X,id:string,raw=LARGE,events=0){
 await addReplayPointer(x.db,{key:`96bulls/org_replay/v1/ws=ws_replay/m=m/h=claude/d=2026-11-01/s=${id}/manifest.json`,digest:"a".repeat(64),physicalSession:id,physicalMachine:"m",canonicalSession:id});
 await projectSessionDirectory(x.db,{orgId:i.orgId,workspaceId:i.workspaceId,harness:i.source,sessionId:id});
 await x.pg.query("UPDATE sessions.session_index SET raw_bytes=$2,event_count=$3 WHERE canonical_session_id=$1",[id,raw,events]);
}
/** Real 54000: capture refuses a canonical scope with more than 10,000 live pointers. */
async function overCapacity(x:X,id:string){
 await x.pg.exec("ALTER TABLE sessions.session_index DISABLE TRIGGER USER");
 await x.pg.query(`INSERT INTO sessions.session_index SELECT (jsonb_populate_record(NULL::sessions.session_index,to_jsonb(p)||jsonb_build_object(
  'session_id',p.session_id||'-'||g,'machine_id','m'||g,'manifest_key',p.manifest_key||'.'||g))).* FROM sessions.session_index p,generate_series(1,10001) g
  WHERE p.canonical_session_id=$1 AND p.session_id=$1`,[id]);
 await x.pg.exec("ALTER TABLE sessions.session_index ENABLE TRIGGER USER");
}
const discover=async(x:X)=>(await x.db.query<{r:any}>("SELECT sessions.discover_session_large_reads() r")).rows[0]!.r;
const claim=async(x:X)=>(await x.db.query<{r:any}>("SELECT sessions.claim_session_large_read($1::uuid,120) r",[N1])).rows[0]!.r;
const route=async(x:X,id:string)=>(await x.db.query<{r:any}>("SELECT sessions.route_session_large_read($1,$2,$3,$4) r",ids(id))).rows[0]!.r;
const errors=async(x:X)=>(await x.pg.query<any>("SELECT session_id,state,sqlstate,message,attempts FROM sessions.session_large_read_scope_errors ORDER BY session_id")).rows;
const jobs=async(x:X)=>(await x.pg.query<any>("SELECT session_id,state,attempts,last_error FROM sessions.session_large_read_jobs ORDER BY session_id")).rows;
const due=(x:X,table:string)=>x.pg.exec(`UPDATE sessions.${table} SET available_at=clock_timestamp()-interval '1 second'`);
const boundary=async(x:X)=>(await x.pg.query<any>(`SELECT p.oid::regprocedure::text sig,n.nspname,pg_get_userbyid(p.proowner) owner,p.prosecdef,p.proconfig,p.proacl::text acl,
 pg_get_function_identity_arguments(p.oid) args,pg_get_function_result(p.oid) result,p.provolatile,p.prolang FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE p.oid IN (${FNS.map(f=>`'${f}'::regprocedure`).join(",")}) ORDER BY 1`)).rows;
const prosrc=async(x:X,fn:string)=>(await x.pg.query<{s:string}>(`SELECT prosrc s FROM pg_proc WHERE oid='${fn}'::regprocedure`)).rows[0]!.s;
const body=(file:string,fn:string,tag:string)=>{const all=[...readFileSync(join(root,"supabase/migrations",file),"utf8").matchAll(new RegExp(`FUNCTION sessions\\.${fn}\\([^]*?AS \\$${tag}\\$([\\s\\S]*?)\\$${tag}\\$;`,"g"))];const m=all.at(-1);if(!m)throw Error(`${fn} missing in ${file}`);return m[1]!};
/** Wraps capture so public.capture_fault rows make it raise a chosen SQLSTATE/message per session. */
const captureFault=(x:X)=>x.pg.exec(`CREATE TABLE public.capture_fault(session_id text PRIMARY KEY,code text NOT NULL,msg text NOT NULL);
 ALTER FUNCTION sessions.capture_session_replay_pages(text,text,text,text) RENAME TO capture_session_replay_pages_real;
 CREATE FUNCTION sessions.capture_session_replay_pages(p_org text,p_workspace text,p_harness text,p_session text) RETURNS jsonb LANGUAGE plpgsql AS $f$
 DECLARE f public.capture_fault%ROWTYPE;
 BEGIN SELECT * INTO f FROM public.capture_fault WHERE session_id=p_session;
  IF FOUND THEN RAISE EXCEPTION USING ERRCODE=f.code,MESSAGE=f.msg; END IF;
  RETURN sessions.capture_session_replay_pages_real(p_org,p_workspace,p_harness,p_session); END $f$;`);
/** Transient per-scope conditions (M1/M2): skipped/deferred, never counted, never recorded. */
const TRANSIENT_SCOPE=[["55000","replay capture: source epoch missing"],["55P03","lock not available"]] as const;
const rollbacks=()=>readdirSync(join(root,"supabase/rollbacks")).filter(f=>f.endsWith(".down.sql")).sort();
/** Roll back every later upstream lane first (descending), then 032000, as the gap-free ledger requires. */
const down=async(x:X)=>{
 try {
  for(const f of rollbacks().filter(f=>f>`${FILE}.down.sql`&&/^\d{14}_/.test(f)).reverse())await x.pg.exec(readFileSync(join(root,"supabase/rollbacks",f),"utf8"));
  await x.pg.exec(readFileSync(join(root,`supabase/rollbacks/${FILE}.down.sql`),"utf8"));
 } catch (error) {
  // 039's explicit transaction must be released after an expected refusal,
  // before the test removes its synthetic later-migration guard and retries.
  await x.pg.exec("ROLLBACK");
  throw error;
 }
};
const migrate=(x:X)=>runMigrations(x.db,{profile:{mode:"local",shim:true,bootstrap:true,verify:true,runtimeRole:"postgres"}});

test("discovery: a 54000 scope is recorded and does not block later scopes or claims",async()=>{
 const x=await fresh();
 for(const id of ["a-poison","b-ok","c-ok"])await addScope(x,id);
 await overCapacity(x,"a-poison");
 await expect(x.db.query("SELECT sessions.enqueue_session_large_read($1,$2,$3,$4)",ids("a-poison"))).rejects.toMatchObject({code:"54000",message:"replay capture: pointer capacity exceeded"});
 expect(await discover(x)).toEqual({busy:false,scanned:3,changed:2,failed:1,skipped:0,cycleComplete:true});
 expect((await jobs(x)).map(j=>j.session_id)).toEqual(["b-ok","c-ok"]);
 expect(await errors(x)).toEqual([{session_id:"a-poison",state:"retrying",sqlstate:"54000",message:"replay capture: pointer capacity exceeded",attempts:1}]);
 expect((await x.pg.query<{a:string[]|null}>("SELECT after_identity a FROM sessions.session_large_read_scan")).rows[0]!.a).toBeNull();
 expect((await claim(x))?.identity?.sessionId).toBe("b-ok");
 expect((await claim(x))?.identity?.sessionId).toBe("c-ok");
},PGLITE_TEST_TIMEOUT*4);

test("discovery: bounded retry of the poisoned scope (30 s backoff, 5 attempts, then failed until repaired)",async()=>{
 const x=await fresh();
 await addScope(x,"a-poison");await addScope(x,"b-ok");await overCapacity(x,"a-poison");
 expect(await discover(x)).toMatchObject({scanned:2,changed:1,failed:1,skipped:0});
 // Inside the backoff window the scope is skipped, not retried.
 expect(await discover(x)).toMatchObject({scanned:2,changed:0,failed:0,skipped:1});
 expect((await x.pg.query<{ok:boolean}>("SELECT available_at>clock_timestamp()+interval '20 seconds' ok FROM sessions.session_large_read_scope_errors")).rows[0]!.ok).toBe(true);
 for(const n of [2,3,4,5]){
  await due(x,"session_large_read_scope_errors");
  expect(await discover(x)).toMatchObject({failed:1,skipped:0});
  expect(await errors(x)).toEqual([expect.objectContaining({attempts:n,state:n>=5?"failed":"retrying",sqlstate:"54000"})]);
 }
 // Failed is terminal for discovery: skipped even when due, attempts stay capped.
 await due(x,"session_large_read_scope_errors");
 expect(await discover(x)).toMatchObject({scanned:2,failed:0,skipped:1});
 expect(await errors(x)).toEqual([expect.objectContaining({attempts:5,state:"failed"})]);
 // Repair: the scope shrinks, still skipped while the row exists; deleting it lets discovery enqueue.
 await x.pg.exec("DELETE FROM sessions.session_index WHERE canonical_session_id='a-poison' AND session_id<>'a-poison'");
 expect(await discover(x)).toMatchObject({skipped:1});
 await x.pg.exec("DELETE FROM sessions.session_large_read_scope_errors");
 expect(await discover(x)).toMatchObject({scanned:2,changed:1,failed:0,skipped:0});
 expect((await jobs(x)).map(j=>j.session_id)).toEqual(["a-poison","b-ok"]);
 expect(await errors(x)).toEqual([]);
},PGLITE_TEST_TIMEOUT*4);

test("discovery: a recovered retrying scope clears its row; transient classes still abort the page; messages stay bounded",async()=>{
 const x=await fresh();
 for(const id of ["a-flaky","b-ok"])await addScope(x,id);
 await captureFault(x);
 // A row-data-bearing message is never stored.
 await x.pg.exec("INSERT INTO public.capture_fault VALUES('a-flaky','P0001','secret transcript text 42')");
 expect(await discover(x)).toMatchObject({scanned:2,changed:1,failed:1});
 expect(await errors(x)).toEqual([{session_id:"a-flaky",state:"retrying",sqlstate:"P0001",message:"unclassified",attempts:1}]);
 // 55000 ambiguous source is recorded like 54000.
 await x.pg.exec("UPDATE public.capture_fault SET code='55000',msg='replay capture: ambiguous canonical source'");
 await due(x,"session_large_read_scope_errors");
 expect(await discover(x)).toMatchObject({failed:1});
 expect(await errors(x)).toEqual([{session_id:"a-flaky",state:"retrying",sqlstate:"55000",message:"replay capture: ambiguous canonical source",attempts:2}]);
 // Transient/server-wide classes are not a scope property: the whole page aborts as before,
 // nothing is recorded and the cursor does not move.
 for(const code of ["40001","53200","57014","08006","XX000"]){
  await x.pg.query("UPDATE public.capture_fault SET code=$1,msg='transient'",[code]);
  await due(x,"session_large_read_scope_errors");
  const before=await errors(x);
  await expect(discover(x)).rejects.toMatchObject({code});
  expect(await errors(x)).toEqual(before);
 }
 // Recovery: the next successful enqueue deletes the row.
 await x.pg.exec("DELETE FROM public.capture_fault");
 await due(x,"session_large_read_scope_errors");
 expect(await discover(x)).toMatchObject({scanned:2,changed:1,failed:0,skipped:0});
 expect(await errors(x)).toEqual([]);
},PGLITE_TEST_TIMEOUT*4);

test("claim: a job whose scope now refuses capture is retried with backoff, then failed, and never wedges other claims",async()=>{
 const x=await fresh();
 for(const id of ["a-grows","b-ok"])await addScope(x,id);
 expect(await discover(x)).toMatchObject({changed:2});
 await overCapacity(x,"a-grows");
 // Oldest available job first: a-grows refuses (54000) but b-ok is still claimed.
 expect((await claim(x))?.identity?.sessionId).toBe("b-ok");
 expect((await jobs(x))[0]).toEqual({session_id:"a-grows",state:"queued",attempts:1,last_error:"capture_54000"});
 expect(await claim(x)).toBeNull(); // a-grows is backing off, b-ok is leased
 for(const n of [2,3,4,5]){
  await due(x,"session_large_read_jobs");
  await x.pg.exec("UPDATE sessions.session_large_read_jobs SET lease_expires_at=clock_timestamp()+interval '2 minutes' WHERE session_id='b-ok'");
  expect(await claim(x)).toBeNull();
  expect((await jobs(x))[0]).toEqual({session_id:"a-grows",state:n>=5?"failed":"queued",attempts:n,last_error:"capture_54000"});
 }
 await due(x,"session_large_read_jobs");
 expect(await claim(x)).toBeNull();
 expect((await jobs(x))[0]).toMatchObject({state:"failed",attempts:5});
},PGLITE_TEST_TIMEOUT*4);

test("discovery: 55000 'source epoch missing' and 55P03 are transient: skipped, never counted, never recorded",async()=>{
 const x=await fresh();
 for(const id of ["a-epoch","b-lock","c-ok"])await addScope(x,id);
 await captureFault(x);
 await x.pg.query("INSERT INTO public.capture_fault VALUES('a-epoch',$1,$2),('b-lock',$3,$4)",TRANSIENT_SCOPE.flat());
 // Well past the fifth-attempt budget: the page completes, c-ok is enqueued, nothing is recorded.
 for(let n=0;n<7;n++){
  expect(await discover(x)).toEqual({busy:false,scanned:3,changed:n===0?1:0,failed:0,skipped:2,cycleComplete:true});
  expect(await errors(x)).toEqual([]);
 }
 expect((await jobs(x)).map(j=>j.session_id)).toEqual(["c-ok"]);
 // An existing retrying row is left exactly as it was (no attempt, no state change, no backoff reset).
 await x.pg.exec("UPDATE public.capture_fault SET code='54000',msg='replay capture: pointer capacity exceeded' WHERE session_id='a-epoch'");
 expect(await discover(x)).toMatchObject({failed:1,skipped:1});
 await x.pg.exec("UPDATE public.capture_fault SET code='55000',msg='replay capture: source epoch missing' WHERE session_id='a-epoch'");
 for(let n=0;n<6;n++){
  await due(x,"session_large_read_scope_errors");
  const before=(await x.pg.query<any>("SELECT available_at::text a FROM sessions.session_large_read_scope_errors")).rows;
  expect(await discover(x)).toMatchObject({failed:0,skipped:2});
  expect(await errors(x)).toEqual([{session_id:"a-epoch",state:"retrying",sqlstate:"54000",message:"replay capture: pointer capacity exceeded",attempts:1}]);
  expect((await x.pg.query<any>("SELECT available_at::text a FROM sessions.session_large_read_scope_errors")).rows).toEqual(before);
 }
 // Only the exact epoch message is transient: any other 55000 is still a counted refusal.
 await x.pg.exec("UPDATE public.capture_fault SET msg='replay capture: ambiguous canonical source' WHERE session_id='a-epoch'");
 expect(await discover(x)).toMatchObject({failed:1,skipped:1});
 expect(await errors(x)).toEqual([expect.objectContaining({session_id:"a-epoch",sqlstate:"55000",attempts:2})]);
 // Recovery: once capture succeeds both scopes are enqueued and the row is cleared.
 await x.pg.exec("DELETE FROM public.capture_fault");
 await due(x,"session_large_read_scope_errors");
 expect(await discover(x)).toMatchObject({scanned:3,changed:2,failed:0,skipped:0});
 expect(await errors(x)).toEqual([]);
},PGLITE_TEST_TIMEOUT*4);

test("claim: 55000 'source epoch missing' and 55P03 back off without counting an attempt and never fail the job",async()=>{
 const x=await fresh();
 for(const id of ["a-epoch","b-lock","c-ok"])await addScope(x,id);
 expect(await discover(x)).toMatchObject({changed:3});
 await captureFault(x);
 await x.pg.query("INSERT INTO public.capture_fault VALUES('a-epoch',$1,$2),('b-lock',$3,$4)",TRANSIENT_SCOPE.flat());
 // Transient jobs are passed over, c-ok is still claimed.
 expect((await claim(x))?.identity?.sessionId).toBe("c-ok");
 expect((await jobs(x)).slice(0,2)).toEqual([{session_id:"a-epoch",state:"queued",attempts:0,last_error:null},{session_id:"b-lock",state:"queued",attempts:0,last_error:null}]);
 // Inside the backoff window they are not retried.
 expect((await x.pg.query<{ok:boolean}>("SELECT bool_and(available_at>clock_timestamp()+interval '20 seconds') ok FROM sessions.session_large_read_jobs WHERE session_id<>'c-ok'")).rows[0]!.ok).toBe(true);
 // Well past the fifth-attempt budget, including from an expired running lease: never counted, never failed.
 await x.pg.exec("UPDATE sessions.session_large_read_jobs SET state='running',lease_nonce=gen_random_uuid(),lease_expires_at=clock_timestamp()-interval '1 second',attempts=1 WHERE session_id='b-lock'");
 for(let n=0;n<7;n++){
  await due(x,"session_large_read_jobs");
  await x.pg.exec("UPDATE sessions.session_large_read_jobs SET lease_expires_at=clock_timestamp()+interval '2 minutes' WHERE session_id='c-ok'");
  expect(await claim(x)).toBeNull();
  expect((await jobs(x)).slice(0,2)).toEqual([{session_id:"a-epoch",state:"queued",attempts:0,last_error:null},{session_id:"b-lock",state:"queued",attempts:1,last_error:null}]);
 }
 expect(await errors(x)).toEqual([]);
 // Only the exact epoch message is transient: any other 55000 counts as an attempt.
 await x.pg.exec("UPDATE public.capture_fault SET msg='replay capture: ambiguous canonical source' WHERE session_id='a-epoch'");
 await due(x,"session_large_read_jobs");
 expect(await claim(x)).toBeNull();
 expect((await jobs(x))[0]).toEqual({session_id:"a-epoch",state:"queued",attempts:1,last_error:"capture_55000"});
 // Recovery: once capture succeeds the deferred job is claimed normally.
 await x.pg.exec("DELETE FROM public.capture_fault WHERE session_id='b-lock'");
 await due(x,"session_large_read_jobs");
 expect((await claim(x))?.identity?.sessionId).toBe("b-lock");
},PGLITE_TEST_TIMEOUT*4);

test("route: 0/NULL counts are not delegated; a measured-large pointer still is",async()=>{
 const x=await fresh();
 await addScope(x,"zero-both",0,0);
 await addScope(x,"zero-bytes",0,5);
 await addScope(x,"zero-events",10,0);
 await addScope(x,"small",10,2);
 for(const id of ["zero-both","zero-bytes","zero-events","small"])expect(await route(x,id)).toEqual({delegated:false});
 expect(await jobs(x)).toEqual([]);
 // Discovery uses the same measured-only selector.
 expect(await discover(x)).toMatchObject({scanned:0,changed:0});
 // session_index declares both NOT NULL; prove the selector itself treats NULL as unmeasured.
 await x.pg.exec("ALTER TABLE sessions.session_index ALTER raw_bytes DROP NOT NULL,ALTER event_count DROP NOT NULL");
 await x.pg.exec("UPDATE sessions.session_index SET raw_bytes=NULL,event_count=NULL WHERE canonical_session_id='zero-both'");
 await x.pg.exec("UPDATE sessions.session_index SET event_count=NULL WHERE canonical_session_id='small'");
 expect(await route(x,"zero-both")).toEqual({delegated:false});
 expect(await route(x,"small")).toEqual({delegated:false});
 expect(await discover(x)).toMatchObject({scanned:0});
 // Measured large by bytes or by events: delegated with a durable queued job, even next to an unmeasured pointer.
 await addScope(x,"big-bytes",LARGE,0);
 await addScope(x,"big-events",0,100001);
 for(const id of ["big-bytes","big-events"]){
  const r=await route(x,id);
  expect(r).toMatchObject({delegated:true,identity:{sessionId:id}});
  expect(typeof r.source.fingerprint).toBe("string");
 }
 expect((await jobs(x)).map(j=>[j.session_id,j.state])).toEqual([["big-bytes","queued"],["big-events","queued"]]);
 // The 55000 handoff contract is unchanged: a refused enqueue still raises, never delegated:false.
 await x.pg.exec(`ALTER FUNCTION sessions.enqueue_session_large_read(text,text,text,text) RENAME TO enqueue_session_large_read_real;
 CREATE FUNCTION sessions.enqueue_session_large_read(p_org text,p_workspace text,p_harness text,p_session text) RETURNS jsonb LANGUAGE sql AS $f$SELECT '{"enqueued":false,"reason":"source_unavailable"}'::jsonb$f$;`);
 await expect(route(x,"big-bytes")).rejects.toMatchObject({code:"55000",message:"large read: durable handoff unavailable"});
 // And capture refusals still propagate unchanged (the consumer classifies 54000 as a logged refusal).
 await overCapacity(x,"zero-both");
 await expect(route(x,"zero-both")).rejects.toMatchObject({code:"54000"});
},PGLITE_TEST_TIMEOUT*4);

test("boundary: signatures, owner, SECURITY DEFINER, search_path and grants are unchanged; the new table is closed",async()=>{
 const x=await fresh();
 const after=await boundary(x);
 expect(after).toHaveLength(3);
 for(const f of after){
  expect(f).toMatchObject({nspname:"sessions",owner:"postgres",prosecdef:true,proconfig:["search_path=pg_catalog, pg_temp"],result:"jsonb",prolang:expect.anything()});
  const acl=(await x.pg.query<{grantee:string;priv:string}>(`SELECT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END grantee,a.privilege_type priv
   FROM pg_proc p,aclexplode(p.proacl) a WHERE p.oid=$1::regprocedure ORDER BY 1`,[f.sig])).rows;
  expect(acl).toEqual([{grantee:"postgres",priv:"EXECUTE"},{grantee:"service_role",priv:"EXECUTE"}]);
 }
 expect(after.map(f=>f.args)).toEqual(["p_nonce uuid, p_lease_seconds integer","","p_org text, p_workspace text, p_harness text, p_session text"]);
 // The exact pre-032000 boundary is restored by the down and kept by the reapply.
 await down(x);
 const before=await boundary(x);
 expect(before).toEqual(after);
 await migrate(x);
 expect(await boundary(x)).toEqual(before);
 const t=(await x.pg.query<any>("SELECT relrowsecurity rls,pg_get_userbyid(relowner) owner,relacl::text acl FROM pg_class WHERE oid='sessions.session_large_read_scope_errors'::regclass")).rows[0];
 expect(t).toMatchObject({rls:true,owner:"postgres"});
 for(const role of ["service_role","anon","authenticated","session_directory_search_worker"]){
  const r=(await x.pg.query<{ok:boolean}>("SELECT has_table_privilege($1,'sessions.session_large_read_scope_errors','SELECT,INSERT,UPDATE,DELETE') ok",[role])).rows[0]!.ok;
  expect([role,r]).toEqual([role,false]);
 }
},PGLITE_TEST_TIMEOUT*4);

test("order: 032000 sorts and applies directly after 031000 and does not depend on its objects",async()=>{
 const x=await fresh();
 // Contiguous around 032000 without assuming it is last: 030000, 031000, 032000 are adjacent and
 // anything after it sorts after 032000 (later lanes such as 036000 may follow).
 const upstream=readdirSync(join(root,"supabase/migrations")).filter(f=>f.endsWith(".sql")).sort();
 const at=upstream.indexOf(`${FILE}.sql`);
 expect(at).toBeGreaterThan(1);
 expect(upstream.slice(at-2,at+1)).toEqual(["20261105030000_canonical_feature_consumer_acks.sql","20261105031000_desktop_catalog_forget_reproject.sql",`${FILE}.sql`]);
 for(const later of upstream.slice(at+1))expect(later>`${FILE}.sql`&&!later.startsWith("20261105032000")).toBe(true);
 const ledger=(await x.pg.query<{name:string}>("SELECT name FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' ORDER BY applied_at,name COLLATE \"C\"")).rows.map(r=>r.name);
 const li=ledger.indexOf(LEDGER);
 expect(li).toBeGreaterThan(1);
 expect(ledger.slice(li-2,li+1)).toEqual(["upstream:20261105030000_canonical_feature_consumer_acks.sql","upstream:20261105031000_desktop_catalog_forget_reproject.sql",LEDGER]);
 // Every ledger row after 032000 is a later migration file, in file order.
 expect(ledger.slice(li+1)).toEqual(upstream.slice(at+1).map(f=>`upstream:${f}`));
 const sql=readFileSync(join(root,"supabase/migrations",`${FILE}.sql`),"utf8")+readFileSync(join(root,`supabase/rollbacks/${FILE}.down.sql`),"utf8");
 for(const dep of ["031000","desktop_catalog"])expect(sql.includes(dep)).toBe(false);
 // No GRANT/REVOKE on any function: CREATE OR REPLACE keeps the original ACL.
 expect(/(GRANT|REVOKE)[^;]*ON\s+FUNCTION/i.test(readFileSync(join(root,"supabase/migrations",`${FILE}.sql`),"utf8"))).toBe(false);
},PGLITE_TEST_TIMEOUT*2);

test("down restores the previous bodies verbatim and the old behaviour; reapply restores the fixes",async()=>{
 const x=await fresh();
 const ledger=async()=>(await x.pg.query<{name:string;sha256:string}>('SELECT name,sha256 FROM sessions.__96bulls_migrations ORDER BY name COLLATE "C"')).rows;
 const beforeLedger=await ledger();
 await addScope(x,"unmeasured",0,0);
 expect(await route(x,"unmeasured")).toEqual({delegated:false});
 // Later-migration guard: refused (55000) while a later upstream step is ledgered.
 const LATER="upstream:20261105099000_rollback_guard_probe.sql";
 await x.pg.query("INSERT INTO sessions.__96bulls_migrations SELECT (jsonb_populate_record(NULL::sessions.__96bulls_migrations,to_jsonb(m)||jsonb_build_object('name',$2::text))).* FROM sessions.__96bulls_migrations m WHERE m.name=$1",[LEDGER,LATER]);
 await expect(down(x)).rejects.toMatchObject({code:"55000"});
 expect((await x.pg.query<{r:string|null}>("SELECT to_regclass('sessions.session_large_read_scope_errors')::text r")).rows[0]!.r).toBe("sessions.session_large_read_scope_errors");
 await x.pg.query("DELETE FROM sessions.__96bulls_migrations WHERE name=$1",[LATER]);
 await down(x);
 expect(await prosrc(x,FNS[0])).toBe(body("20261105021000_session_large_read_jobs.sql","route_session_large_read","route"));
 expect(await prosrc(x,FNS[1])).toBe(body("20261105021000_session_large_read_jobs.sql","discover_session_large_reads","discover"));
 expect(await prosrc(x,FNS[2])).toBe(body("20261105026000_canonical_history_lineage.sql","claim_session_large_read","claim"));
 expect((await x.pg.query<{r:string|null}>("SELECT to_regclass('sessions.session_large_read_scope_errors')::text r")).rows[0]!.r).toBeNull();
 expect((await x.pg.query<{n:number}>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1",[LEDGER])).rows[0]!.n).toBe(0);
 // Old behaviour: an unmeasured scope is delegated again.
 expect(await route(x,"unmeasured")).toMatchObject({delegated:true});
 await x.pg.exec("DELETE FROM sessions.session_large_read_jobs");
 // Reapply through the real migrator: 032000 (and any later lane rolled back above) is applied, the fixes are back.
 await migrate(x);
 expect((await x.pg.query<{n:number}>("SELECT count(*)::int n FROM sessions.__96bulls_migrations WHERE name=$1",[LEDGER])).rows[0]!.n).toBe(1);
 expect(await route(x,"unmeasured")).toEqual({delegated:false});
 expect(await prosrc(x,FNS[0])).toBe(body(`${FILE}.sql`,"route_session_large_read","route"));
 expect(await prosrc(x,FNS[1])).toBe(body(`${FILE}.sql`,"discover_session_large_reads","discover"));
 expect(await prosrc(x,FNS[2])).toBe(body(`${FILE}.sql`,"claim_session_large_read","claim"));
 expect(await ledger()).toEqual(beforeLedger);
 // A second migrate is a no-op.
 await migrate(x);
 expect(await ledger()).toEqual(beforeLedger);
},PGLITE_TEST_TIMEOUT*4);
