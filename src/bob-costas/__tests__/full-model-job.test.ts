import{test,expect}from"bun:test";
import{modelPoolConfig,modelStore,runFullModelJob}from"../full-model-job.ts";
import{historyDerivedKey,type HistorySourceReference}from"../../shared/history-source-authority.ts";
import{derivedSchemaAssets}from"../../session-pipeline/derived-schema.ts";
const url="postgres://bulls96_session_model.pyamkrhoyszgzirqqffq:fixture@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require";
test("dedicated model target pins role/project/host/TLS and rejects privilege or URL-option fallback",()=>{
 const c=modelPoolConfig(url);expect(c.user).toBe("bulls96_session_model.pyamkrhoyszgzirqqffq");expect((c.ssl as any).rejectUnauthorized).toBe(true);expect(c.connectionTimeoutMillis).toBe(1000);expect(c.max).toBe(1);
 for(const bad of[url.replace("bulls96_session_model","relayhistory_owner"),url.replace("pyamkrhoyszgzirqqffq","foreign"),url.replace("5432","6543"),url.replace("sslmode=require","sslmode=disable"),url+"&sslrootcert=/fake",url.replace("aws-0-us-east-1","aws-0-us-west-1")])expect(()=>modelPoolConfig(bad)).toThrow();
});
test("model store only reads qualified graphs/slices and immutably writes final/slice objects",async()=>{
 const id="12345678-1234-1234-1234-123456789abc",captured={derivedAnchorKey:"96bulls/org/upload-generations/g="+id+"/v1/ws=ws/m=history_scope/h=claude/d=2026/11/01/s=session/publications/"+id+"/manifest.json"}as HistorySourceReference;
 let gets=0,puts=0,others=0;
 const store=modelStore({get:async()=>{gets++;return new ReadableStream()},put:async()=>{puts++;return{}},delete:async()=>{others++},list:async()=>{others++}}as never);
 const key=historyDerivedKey(captured,"bob-slice",id);
 await store.get(key as never);
 await store.get((captured.derivedAnchorKey.slice(0,-13)+"derived/replay/"+"a".repeat(64)+"/"+"b".repeat(64)+".json")as never);
 await store.put(key as never,new Uint8Array([1]),{ifNoneMatch:true});
 await expect(store.put(key as never,new Uint8Array([1]),{})).rejects.toMatchObject({code:"model_store_write_scope"});
 await expect(store.get(captured.derivedAnchorKey as never)).rejects.toMatchObject({code:"model_store_scope"});
 await expect(store.delete(key as never)).rejects.toMatchObject({code:"model_store_operation"});
 await expect(store.list({prefix:"96bulls"}as never)).rejects.toMatchObject({code:"model_store_operation"});
 expect({gets,puts,others}).toEqual({gets:2,puts:1,others:0});
});
test("default off/model off and wrong compiled generation perform no secret, pool or provider IO",async()=>{
 let io=0;
 const overrides={secret:async()=>{io++;throw Error("unexpected secret")},pool:()=>{io++;throw Error("unexpected pool")},fetcher:(async()=>{io++;throw Error("unexpected fetch")})as any};
 expect((await runFullModelJob({},()=>300000,overrides)).enabled).toBe(false);
 await expect(runFullModelJob({SESSION_FULL_MODEL_ENABLED:"true",BOB_COSTAS_MODEL:"off"},()=>300000,overrides)).rejects.toMatchObject({code:"model_job_provider_required"});
 const assets=await derivedSchemaAssets();
 const env={SESSION_FULL_MODEL_ENABLED:"true",BOB_COSTAS_MODEL:"fixture-model",AWS_REGION:"us-east-1",HOOPDB_DB_MODE:"supabase",PIPELINE_RAW_S3_BUCKET:"fixture-bucket",SESSION_MODEL_KMS_KEY_ARN:"arn:aws:kms:us-east-1:941433935742:key/11111111-1111-4111-8111-111111111111",SESSION_MODEL_SECRET_ARN:"arn:aws:secretsmanager:us-east-1:941433935742:secret:96bulls/fixture/session-model-abcdef",SESSION_MODEL_EXPECT_GENERATION:"f".repeat(64)};
 await expect(runFullModelJob(env,()=>300000,{...overrides,assets:async()=>assets})).rejects.toMatchObject({code:"model_compiled_generation"});
 expect(io).toBe(0);
});

test("dedicated model residual gate uses its exact static registry without broadening the app runtime",async()=>{
 const {PGlite}=await import("@electric-sql/pglite"),{sessionsFunctionGateSql}=await import("../../shared/db.ts"),{MODEL_WORKER_FUNCTIONS}=await import("../../shared/model-worker-contract.ts");
 const pg=new PGlite();try{
 await pg.exec("CREATE ROLE bulls96_session_model;CREATE SCHEMA sessions;REVOKE ALL ON SCHEMA sessions FROM PUBLIC;GRANT USAGE ON SCHEMA sessions TO bulls96_session_model");
 for(const signature of MODEL_WORKER_FUNCTIONS)await pg.exec("CREATE FUNCTION "+signature.replace("(jsonb)","(p jsonb)")+"RETURNS jsonb LANGUAGE sql AS $$SELECT '{}'::jsonb$$;REVOKE ALL ON FUNCTION "+signature+" FROM PUBLIC;GRANT EXECUTE ON FUNCTION "+signature+" TO bulls96_session_model");
 await pg.exec("SET ROLE bulls96_session_model");
 await pg.exec(sessionsFunctionGateSql(MODEL_WORKER_FUNCTIONS));
 await expect(pg.exec(sessionsFunctionGateSql([]))).rejects.toThrow("live gate: sessions functions executable");
 await pg.exec("RESET ROLE;CREATE FUNCTION sessions.unexpected_model_cap()RETURNS void LANGUAGE sql AS $$SELECT$$;REVOKE ALL ON FUNCTION sessions.unexpected_model_cap() FROM PUBLIC;GRANT EXECUTE ON FUNCTION sessions.unexpected_model_cap() TO bulls96_session_model;SET ROLE bulls96_session_model");
 await expect(pg.exec(sessionsFunctionGateSql(MODEL_WORKER_FUNCTIONS))).rejects.toThrow("sessions.unexpected_model_cap()");
 expect(()=>sessionsFunctionGateSql(["sessions.evil();DROP ROLE postgres"])).toThrow("function_gate_registry_invalid");
 }finally{await pg.close()}
});
