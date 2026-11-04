/** Proposed role policy only; no database/operator grants are performed here. */
import{test,expect}from"bun:test";
import{PGlite}from"@electric-sql/pglite";
import{readFile}from"node:fs/promises";
import{MODEL_WORKER_FUNCTIONS,MODEL_WORKER_ROLE}from"../../shared/model-worker-contract.ts";
const sql=async(name:string)=>readFile(new URL("../../../supabase/bootstrap/"+name,import.meta.url),"utf8");
async function fixture(){
 const db=new PGlite();
 await db.exec("CREATE SCHEMA sessions;REVOKE ALL ON SCHEMA sessions FROM PUBLIC;CREATE TABLE sessions.fixture_payload(secret text);CREATE SEQUENCE sessions.fixture_seq;");
 await db.exec(await sql("session_history_model_worker.sql"));
 for(const f of MODEL_WORKER_FUNCTIONS){
  const name=f.slice(0,f.indexOf("("));
  await db.exec("CREATE FUNCTION "+name+"(p jsonb)RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$SELECT '{}'::jsonb$$;REVOKE ALL ON FUNCTION "+f+" FROM PUBLIC;GRANT EXECUTE ON FUNCTION "+f+" TO "+MODEL_WORKER_ROLE);
 }
 await db.exec("CREATE FUNCTION sessions.private_fixture()RETURNS void LANGUAGE sql AS $$SELECT$$;REVOKE ALL ON FUNCTION sessions.private_fixture() FROM PUBLIC;GRANT USAGE ON SCHEMA sessions TO "+MODEL_WORKER_ROLE);
 return{db,verify:async()=>db.exec(await sql("session_history_model_verify.sql"))};
}
test("proposed model role has only seven SD calls and no effective table/column/sequence/private access",async()=>{
 const f=await fixture();try{
 await f.verify();await f.db.exec(await sql("session_history_model_worker.sql"));
 await f.db.exec("SET ROLE "+MODEL_WORKER_ROLE);
 await expect(f.db.query("SELECT * FROM sessions.fixture_payload")).rejects.toThrow("permission denied");
 await expect(f.db.query("SELECT nextval('sessions.fixture_seq')")).rejects.toThrow("permission denied");
 await expect(f.db.query("SELECT sessions.private_fixture()")).rejects.toThrow("permission denied");
 expect((await f.db.query("SELECT sessions.session_history_model_claim('{}'::jsonb) AS result")).rows).toEqual([{result:{}}]);
 }finally{await f.db.close()}
},10000);
test("bare and approved LOGIN profiles are distinct; drift is never repaired by verifier",async()=>{
 const f=await fixture();try{
 await f.db.exec("ALTER ROLE "+MODEL_WORKER_ROLE+" LOGIN;ALTER ROLE "+MODEL_WORKER_ROLE+" SET statement_timeout='5s';ALTER ROLE "+MODEL_WORKER_ROLE+" SET lock_timeout='2s';ALTER ROLE "+MODEL_WORKER_ROLE+" SET idle_in_transaction_session_timeout='10s'");
 await f.verify();
 await f.db.exec("ALTER ROLE "+MODEL_WORKER_ROLE+" SET lock_timeout='3s'");
 await expect(f.verify()).rejects.toThrow("LOGIN timeout profile differs");
 expect((await f.db.query<any>("SELECT rolconfig FROM pg_roles WHERE rolname=$1",[MODEL_WORKER_ROLE])).rows[0]?.rolconfig).toContain("lock_timeout=3s");
 }finally{await f.db.close()}
},10000);
test("PUBLIC data leak, role membership, schema creation and extra function EXECUTE all refuse",async()=>{
 const cases=[
  ["GRANT SELECT(secret) ON sessions.fixture_payload TO PUBLIC","privilege on"],
  ["GRANT USAGE ON SEQUENCE sessions.fixture_seq TO PUBLIC","sequence privilege"],
  ["CREATE ROLE elevated;GRANT elevated TO "+MODEL_WORKER_ROLE,"membership or settings"],
  ["GRANT CREATE ON SCHEMA sessions TO "+MODEL_WORKER_ROLE,"schema privileges"],
  ["GRANT EXECUTE ON FUNCTION sessions.private_fixture() TO "+MODEL_WORKER_ROLE,"unexpected EXECUTE"],
 ];
 for(const[mutant,reason]of cases){
  const f=await fixture();try{await f.db.exec(mutant!);await expect(f.verify()).rejects.toThrow(reason!);}finally{await f.db.close()}
 }
},15000);
