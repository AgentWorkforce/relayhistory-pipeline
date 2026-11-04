/** Actual630 SQL lease/rollback controls.620 source authority is a deliberately
 * minimal OWNED fixture port here; this is NOT620 integration/readiness proof. */
import{test,expect}from"bun:test";
import{PGlite}from"@electric-sql/pglite";
import{readFile}from"node:fs/promises";
import{enqueueFullModelWork,claimFullModelWork,heartbeatFullModelWork,checkpointFullModelWork,failFullModelWork,discoverFullModelScopes,modelWorkConfig}from"../full-model-ledger.ts";
import type{HistorySourceReference}from"../../shared/history-source-authority.ts";
const GEN="a".repeat(64),HASH="b".repeat(64),ID="12345678-1234-1234-1234-123456789abc";
const captured:HistorySourceReference={identity:{orgId:"org",workspaceId:"ws",source:"claude",sessionId:"session"},source:{epoch:"9007199254740993",fingerprint:HASH},authorityId:ID,maskSha256:HASH,recordCount:"1",schemaGeneration:GEN,derivedAnchorKey:"fixture-anchor"};
async function fixture(){
 const pg=new PGlite(),db={query:async<T>(q:string,p?:unknown[])=>pg.query<T>(q,p)},graph={version:1 as const,identity:captured.identity,source:captured.source,generation:HASH,prefix:"fixture/graph/",index:{key:"fixture/graph/index.json",sha256:HASH,bytes:123}},config=modelWorkConfig("fixture-model","fixture-key",graph);
 await pg.exec(`
 CREATE ROLE anon;CREATE ROLE authenticated;CREATE ROLE service_role;CREATE SCHEMA sessions;
 CREATE TABLE sessions.ingest_history_scope_versions(evidence jsonb PRIMARY KEY,authority_id uuid,org_id text,workspace_id text,harness text,session_id text,source_epoch bigint,source_fingerprint text,mask_sha256 text,state text,record_count bigint,derived_anchor_key text);
 CREATE TABLE sessions.ingest_history_scope_current(authority_id uuid,org_id text,workspace_id text,harness text,session_id text,ready boolean);
 CREATE TABLE sessions.session_replay_publications(org_id text,workspace_id text,harness text,session_id text,epoch bigint,published_epoch bigint,index_key text,source_snapshot jsonb,source_fingerprint text);
 CREATE TABLE sessions.test_model_callbacks(kind text PRIMARY KEY,payload jsonb);
 CREATE TABLE sessions.test_model_graph(publication jsonb);
 CREATE TABLE sessions.test_callback_behavior(fail_state boolean NOT NULL DEFAULT false);
 INSERT INTO sessions.test_callback_behavior DEFAULT VALUES;
 CREATE FUNCTION sessions.ingest_require_generation(g text)RETURNS void LANGUAGE plpgsql AS $$BEGIN IF g IS DISTINCT FROM '${GEN}' THEN RAISE EXCEPTION 'wrong generation';END IF;END$$;
 CREATE FUNCTION sessions.ingest_history_source_current(e jsonb,l boolean)RETURNS sessions.ingest_history_scope_versions LANGUAGE plpgsql AS $$
 DECLARE s sessions.ingest_history_scope_versions;BEGIN
 PERFORM sessions.ingest_require_generation(e->>'generation');
 IF l THEN SELECT * INTO s FROM sessions.ingest_history_scope_versions WHERE evidence=e FOR SHARE;
 ELSE SELECT * INTO s FROM sessions.ingest_history_scope_versions WHERE evidence=e;END IF;RETURN s;END$$;
 CREATE FUNCTION sessions.session_history_source_lock(e jsonb)RETURNS jsonb LANGUAGE sql AS $$SELECT jsonb_build_object('current',(sessions.ingest_history_source_current(e,true)).evidence IS NOT NULL)$$;
 CREATE FUNCTION sessions.session_history_summary_publish(p jsonb)RETURNS jsonb LANGUAGE plpgsql AS $$BEGIN
 INSERT INTO sessions.test_model_callbacks VALUES(p->'pointer'->>'kind',p) ON CONFLICT(kind)DO UPDATE SET payload=excluded.payload;
 RETURN '{"applied":true}'::jsonb;END$$;
 CREATE FUNCTION sessions.session_history_state_publish(p jsonb)RETURNS jsonb LANGUAGE plpgsql AS $$BEGIN
 IF(SELECT fail_state FROM sessions.test_callback_behavior)THEN RETURN '{"applied":false}'::jsonb;END IF;
 INSERT INTO sessions.test_model_callbacks VALUES('state',p) ON CONFLICT(kind)DO UPDATE SET payload=excluded.payload;
 RETURN '{"applied":true}'::jsonb;END$$;
 `);
 const evidence={generation:GEN,identity:captured.identity,source:captured.source,authorityId:ID,maskSha256:HASH};
 await db.query("INSERT INTO sessions.ingest_history_scope_versions(evidence,record_count,derived_anchor_key) VALUES($1::jsonb,1,'fixture-anchor')",[JSON.stringify(evidence)]);
 await db.query("INSERT INTO sessions.test_model_graph VALUES($1::jsonb)",[JSON.stringify(graph)]);
 await pg.exec(await readFile(new URL("../../../supabase/bootstrap/session_history_model_worker.sql",import.meta.url),"utf8"));
 for(const name of ["capture","register_object"])await pg.exec("CREATE FUNCTION sessions.session_history_"+name+"(p jsonb)RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$SELECT '{}'::jsonb$$;REVOKE ALL ON FUNCTION sessions.session_history_"+name+"(jsonb) FROM PUBLIC");
 await pg.exec("CREATE FUNCTION sessions.session_history_replay_descriptor(p jsonb)RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$SELECT jsonb_build_object('current',true,'publication',(SELECT publication FROM sessions.test_model_graph))$$;REVOKE ALL ON FUNCTION sessions.session_history_replay_descriptor(jsonb) FROM PUBLIC");
 await pg.exec(await readFile(new URL("../../../supabase/migrations/20261104063000_history_model_work.sql",import.meta.url),"utf8"));
 return{pg,db,config,evidence,async enqueue(){return enqueueFullModelWork(db,captured,config)}};
}
test("actual630 idempotent durable checkpoint survives re-wake; lease nonce/version cannot be borrowed",async()=>{
 const f=await fixture();try{
 const first=await f.enqueue(),again=await f.enqueue();expect(again).toEqual(first);
 const t=await claimFullModelWork(f.db,GEN,"fixture-model");expect(t).not.toBeNull();if(!t)throw Error("ticket");
 expect(await claimFullModelWork(f.db,GEN,"fixture-model")).toBeNull();
 expect(await heartbeatFullModelWork(f.db,{...t,leaseToken:ID})).toBe(false);
 const next=structuredClone(t.checkpoint);next.inputComplete=true;
 expect(await checkpointFullModelWork(f.db,t,next)).toBe(true);
 expect(await checkpointFullModelWork(f.db,t,next)).toBe(false);
 await f.enqueue();
 const resumed=await claimFullModelWork(f.db,GEN,"fixture-model");expect(resumed?.checkpointVersion).toBe("1");expect(resumed?.checkpoint.inputComplete).toBe(true);expect(resumed?.leaseToken).not.toBe(t.leaseToken);
 }finally{await f.pg.close()}
},10000);
test("actual630 BOTH summaries, state and complete checkpoint rollback together after final callback failure",async()=>{
 const f=await fixture();try{
 await f.enqueue();const t=(await claimFullModelWork(f.db,GEN,"fixture-model"))!;const next={...t.checkpoint,inputComplete:true};
 const done={pointers:(["short","long"]as const).map(kind=>({kind,status:"empty"as const,objectKey:null,objectSha256:null,objectBytes:null,keyId:null,model:null,builtAt:"2026-11-04T00:00:00Z"})),state:"unknown"as const,classifiedAt:"2026-11-04T00:00:00Z"};
 await f.pg.exec("UPDATE sessions.test_callback_behavior SET fail_state=true");
 expect(await checkpointFullModelWork(f.db,t,next,done)).toBe(false);
 expect((await f.db.query("SELECT * FROM sessions.test_model_callbacks")).rows).toHaveLength(0);
 expect((await f.db.query<any>("SELECT checkpoint_version::text,status FROM sessions.history_model_work")).rows[0]).toEqual({checkpoint_version:"0",status:"pending"});
 await f.pg.exec("UPDATE sessions.test_callback_behavior SET fail_state=false");
 expect(await checkpointFullModelWork(f.db,t,next,done)).toBe(true);
 expect((await f.db.query("SELECT * FROM sessions.test_model_callbacks")).rows).toHaveLength(3);
 expect((await f.db.query<any>("SELECT status FROM sessions.history_model_work")).rows[0]?.status).toBe("complete");
 }finally{await f.pg.close()}
},10000);
test("actual630 final wall-clock expiry rolls back callbacks AND checkpoint after blocked writes",async()=>{
 const f=await fixture();try{
 await f.enqueue();const t=(await claimFullModelWork(f.db,GEN,"fixture-model"))!;
 await f.pg.exec(`CREATE FUNCTION sessions.test_model_delay()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_sleep(.08);RETURN NEW;END$$;
 CREATE TRIGGER test_delay BEFORE UPDATE ON sessions.history_model_work FOR EACH ROW EXECUTE FUNCTION sessions.test_model_delay();`);
 // Disable the test trigger only while arranging the near-expiry fixture.
 await f.pg.exec("ALTER TABLE sessions.history_model_work DISABLE TRIGGER test_delay;UPDATE sessions.history_model_work SET lease_until=clock_timestamp()+interval '30 milliseconds';ALTER TABLE sessions.history_model_work ENABLE TRIGGER test_delay");
 expect(await heartbeatFullModelWork(f.db,t)).toBe(false);
 const row=(await f.db.query<any>("SELECT checkpoint_version::text,lease_token::text FROM sessions.history_model_work")).rows[0];expect(row?.checkpoint_version).toBe("0");expect(row?.lease_token).toBe(t.leaseToken);
 await f.pg.exec("ALTER TABLE sessions.history_model_work DISABLE TRIGGER test_delay;UPDATE sessions.history_model_work SET lease_until=clock_timestamp()+interval '30 milliseconds';ALTER TABLE sessions.history_model_work ENABLE TRIGGER test_delay");
 const next={...t.checkpoint,inputComplete:true};
 const done={pointers:(["short","long"]as const).map(kind=>({kind,status:"empty"as const,objectKey:null,objectSha256:null,objectBytes:null,keyId:null,model:null,builtAt:"2026-11-04T00:00:00Z"})),state:"unknown"as const,classifiedAt:"2026-11-04T00:00:00Z"};
 expect(await checkpointFullModelWork(f.db,t,next,done)).toBe(false);
 expect((await f.db.query("SELECT * FROM sessions.test_model_callbacks")).rows).toHaveLength(0);
 expect((await f.db.query<any>("SELECT checkpoint_version::text,status FROM sessions.history_model_work")).rows[0]).toEqual({checkpoint_version:"0",status:"pending"});
 }finally{await f.pg.close()}
},10000);
test("stale source/no grant/wrong generation refuse; no implicit owner or service-role fallback",async()=>{
 const f=await fixture();try{
 await f.enqueue();await f.pg.exec("DELETE FROM sessions.ingest_history_scope_versions");
 expect(await f.enqueue()).toEqual({current:false});expect(await claimFullModelWork(f.db,GEN,"fixture-model")).toBeNull();
 await expect(claimFullModelWork(f.db,"c".repeat(64),"fixture-model")).rejects.toThrow("wrong generation");
 await f.pg.exec("SET ROLE service_role");
 await expect(f.db.query("SELECT sessions.session_history_model_claim($1::jsonb)",[JSON.stringify({generation:GEN,leaseSeconds:120,model:"fixture-model"})])).rejects.toThrow("permission denied");
 await expect(f.db.query("SELECT * FROM sessions.history_model_work")).rejects.toThrow("permission denied");
 }finally{await f.pg.close()}
},10000);

test("permanent provider failure is durable and owner-only exact-source retry revokes old nonce",async()=>{
 const f=await fixture();try{
 await f.enqueue();const t=(await claimFullModelWork(f.db,GEN,"fixture-model"))!;
 expect(await failFullModelWork(f.db,t,{code:"model_refusal",retryable:false})).toBe(true);
 expect((await f.db.query<any>("SELECT status,last_error_code FROM sessions.history_model_work")).rows[0]).toEqual({status:"failed",last_error_code:"model_refusal"});
 expect(await claimFullModelWork(f.db,GEN,"fixture-model")).toBeNull();
 expect((await f.db.query<any>("SELECT sessions.ingest_history_model_retry($1::jsonb) AS r",[JSON.stringify({workId:t.workId,generation:GEN})])).rows[0]?.r).toEqual({retried:true});
 const retry=(await claimFullModelWork(f.db,GEN,"fixture-model"))!;expect(retry.leaseToken).not.toBe(t.leaseToken);expect(retry.checkpointVersion).toBe("2");
 expect(await failFullModelWork(f.db,t,{code:"model_refusal",retryable:false})).toBe(false);
 }finally{await f.pg.close()}
},10000);

test("actual630 derives namespace and row count from locked source instead of caller metadata",async()=>{
 const f=await fixture();try{
 for(const changed of[{...captured,derivedAnchorKey:"foreign-anchor"},{...captured,recordCount:"2"},{...captured,untrusted:"extra"}]){
  await expect(enqueueFullModelWork(f.db,changed as never,f.config)).rejects.toMatchObject({code:"22023"});
 }
 expect((await f.db.query("SELECT * FROM sessions.history_model_work")).rows).toHaveLength(0);
 expect((await f.enqueue()).current).toBe(true);
 }finally{await f.pg.close()}
},10000);


test("new schema generation re-wakes only expired pending work without adopting its nonce or frontier",async()=>{
 const f=await fixture();try{
 await f.enqueue();const t=(await claimFullModelWork(f.db,GEN,"fixture-model"))!;
 const old="d".repeat(64),e={...f.evidence,generation:old};
 await f.db.query("UPDATE sessions.history_model_work SET evidence=$1::jsonb,captured=jsonb_set(captured,'{schemaGeneration}',to_jsonb($2::text)),work_key=encode(sha256(convert_to(jsonb_build_array('history-model-work-v1',$1::jsonb,config)::text,'UTF8')),'hex')",[JSON.stringify(e),old]);
 expect(await discoverFullModelScopes(f.db,GEN)).toEqual([]);
 expect((await f.db.query<any>("SELECT status FROM sessions.history_model_work")).rows[0]?.status).toBe("pending");
 await f.pg.exec("UPDATE sessions.history_model_work SET lease_until=clock_timestamp()-interval '1 second'");
 const wake=await discoverFullModelScopes(f.db,GEN);expect(wake).toHaveLength(1);
 expect((await f.db.query<any>("SELECT status,last_error_code FROM sessions.history_model_work")).rows[0]).toEqual({status:"canceled",last_error_code:"schema_generation_changed"});
 expect(await heartbeatFullModelWork(f.db,t)).toBe(false);
 await enqueueFullModelWork(f.db,captured,f.config,wake[0]!.wakeToken);
 const fresh=(await claimFullModelWork(f.db,GEN,"fixture-model"))!;
 expect(fresh.workId).not.toBe(t.workId);expect(fresh.leaseToken).not.toBe(t.leaseToken);
 expect(fresh.checkpoint.cursor).toEqual({rowOrdinal:"0",utf16Offset:0});expect(fresh.checkpoint.calls).toBe("0");
 expect((await f.db.query("SELECT * FROM sessions.history_model_wakeups")).rows).toHaveLength(0);
 }finally{await f.pg.close()}
},10000);
