import {test,expect} from "bun:test";
import {gcJobConfig,gcTargetConfig,gcPoolConfig,bufferedKeyAllowed,scopedGcStore,runBufferedGcJob} from "../buffered-gc-job.ts";
import {gcEnabled,handler} from "../buffered-gc-lambda.ts";
// Synthetic deployment identity (AWS documentation account, placeholder Supabase ref).
const ACCOUNT="111122223333",REF="abcdefghijklmnopqrst";
const codeOf=(f:()=>unknown)=>{try{f()}catch(e){return(e as {code?:string}).code}return"no_throw"};
const env={AWS_REGION:"us-east-1",HOOPDB_DB_MODE:"supabase",INGEST_GC_BUCKET:"synthetic-bucket",
 INGEST_GC_AWS_ACCOUNT_ID:ACCOUNT,INGEST_GC_SUPABASE_PROJECT_REF:REF,
 INGEST_GC_KMS_KEY_ARN:`arn:aws:kms:us-east-1:${ACCOUNT}:key/11111111-1111-4111-8111-111111111111`,
 INGEST_GC_SECRET_ARN:`arn:aws:secretsmanager:us-east-1:${ACCOUNT}:secret:96bulls/prod/buffered-gc-abcdef`,
 INGEST_GC_EXPECT_GENERATION:"a".repeat(64)};
const url=`postgresql://bulls96_session_ingest.${REF}:synthetic-unused@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require`;
test("default-off entry exits before credentials/database/objects and rejects ambiguous switches",async()=>{
 const old=process.env.INGEST_GC_ENABLED;delete process.env.INGEST_GC_ENABLED;
 try{expect(await handler()).toEqual({enabled:false});expect(gcEnabled({})).toBe(false);expect(()=>gcEnabled({INGEST_GC_ENABLED:"1"})).toThrow()}
 finally{if(old!==undefined)process.env.INGEST_GC_ENABLED=old}
});
test("job requires pinned target, generation and bounded budgets",()=>{
 expect(gcJobConfig(env).budgetMs).toBe(45000);
 for(const patch of [{AWS_REGION:"us-west-2"},{HOOPDB_DB_MODE:"local"},{INGEST_GC_EXPECT_GENERATION:""},{INGEST_GC_BUDGET_MS:"60000"},{INGEST_GC_BATCH_LIMIT:"100"},{INGEST_GC_KMS_KEY_ARN:env.INGEST_GC_KMS_KEY_ARN.replace(ACCOUNT,"111111111111")}])expect(()=>gcJobConfig({...env,...patch})).toThrow();
});
test("dedicated ingest login uses pinned TLS; URL options cannot disable it",()=>{
 const c=gcPoolConfig(JSON.stringify({hoopdb_url:url}),REF);expect(c.user).toBe(`bulls96_session_ingest.${REF}`);expect((c.ssl as any).rejectUnauthorized).toBe(true);expect(c.connectionTimeoutMillis).toBe(1000);
 for(const x of [url.replace("bulls96_session_ingest","relayhistory_owner"),url.replace(REF,"other"),url.replace(":5432",":6543"),url+"&sslmode=disable",url+"&application_name=leak",url.replace("aws-0-us-east-1.pooler.supabase.com","localhost")])expect(()=>gcPoolConfig(JSON.stringify({hoopdb_url:x}),REF)).toThrow();
 expect(()=>gcPoolConfig(JSON.stringify({hoopdb_url:url,other:"unused"}),REF)).toThrow();
});
test("deployment identity is required configuration with no default target",()=>{
 const c=gcJobConfig(env);expect(c.accountId).toBe(ACCOUNT);expect(c.supabaseProjectRef).toBe(REF);
 for(const name of ["INGEST_GC_AWS_ACCOUNT_ID","INGEST_GC_SUPABASE_PROJECT_REF"]){
  const{[name]:_,...rest}=env as Record<string,string>;
  expect(codeOf(()=>gcJobConfig(rest))).toBe("config_required");
  expect(codeOf(()=>gcJobConfig({...env,[name]:""}))).toBe("config_required");
 }
 expect(codeOf(()=>gcTargetConfig({},{account:"A",supabaseRef:"R"}))).toBe("config_required");
 for(const patch of [{INGEST_GC_AWS_ACCOUNT_ID:"11112222333"},{INGEST_GC_AWS_ACCOUNT_ID:".*"},{INGEST_GC_AWS_ACCOUNT_ID:"1111222233334"},
  {INGEST_GC_SUPABASE_PROJECT_REF:"short"},{INGEST_GC_SUPABASE_PROJECT_REF:"ABCDEFGHIJKLMNOPQRST"},{INGEST_GC_SUPABASE_PROJECT_REF:"abcdefghij.lmnopqrst"}])
  expect(codeOf(()=>gcJobConfig({...env,...patch}))).toBe("target_invalid");
});
test("configured account pins every ARN and configured ref pins the database login",()=>{
 const other="444455556666";
 expect(codeOf(()=>gcJobConfig({...env,INGEST_GC_AWS_ACCOUNT_ID:other}))).toBe("target_invalid");
 const moved={...env,INGEST_GC_AWS_ACCOUNT_ID:other,INGEST_GC_KMS_KEY_ARN:env.INGEST_GC_KMS_KEY_ARN.replace(ACCOUNT,other),INGEST_GC_SECRET_ARN:env.INGEST_GC_SECRET_ARN.replace(ACCOUNT,other)};
 expect(gcJobConfig(moved).kmsKeyArn).toContain(other);
 expect(codeOf(()=>gcJobConfig({...moved,INGEST_GC_SECRET_ARN:env.INGEST_GC_SECRET_ARN}))).toBe("target_invalid");
 const ref2="zyxwvutsrqponmlkjihg";
 expect(codeOf(()=>gcPoolConfig(JSON.stringify({hoopdb_url:url}),ref2))).toBe("database_target");
 expect(gcPoolConfig(JSON.stringify({hoopdb_url:url.replace(REF,ref2)}),ref2).user).toBe(`bulls96_session_ingest.${ref2}`);
 for(const bad of ["",undefined as unknown as string,"abcdefghijklmnopqrs."])expect(codeOf(()=>gcPoolConfig(JSON.stringify({hoopdb_url:url}),bad))).toBe("target_invalid");
});
test("job passes the configured ref to the database login before any query",async()=>{
 let poolUser="";
 await expect(runBufferedGcJob({...env,INGEST_GC_EXPECT_GENERATION:"b".repeat(64)},{
  assets:async()=>({expected:{names:["x"],generation:"b".repeat(64)},verifiers:{searchWorker:"x",tokenVerifier:"x",ingestSeeder:"x",ingest:"x"}}),
  secret:async()=>JSON.stringify({hoopdb_url:url}),
  pool:(c)=>{poolUser=String(c.user);throw Object.assign(Error("stop_after_pool_config"),{code:"stop_after_pool_config"})},
 })).rejects.toMatchObject({code:"stop_after_pool_config"});
 expect(poolUser).toBe(`bulls96_session_ingest.${REF}`);
});
test("compiled generation mismatch refuses before secret retrieval",async()=>{
 let secrets=0;
 await expect(runBufferedGcJob(env,{assets:async()=>({expected:{names:["x"],generation:"b".repeat(64)},verifiers:{searchWorker:"x",tokenVerifier:"x",ingestSeeder:"x",ingest:"x"}}),secret:async()=>{secrets++;throw Error("unused")}})).rejects.toMatchObject({code:"compiled_generation_mismatch"});
 expect(secrets).toBe(0);
});
test("buffer-only store refuses canonical/object enumeration and invalid paths before transport",async()=>{
 let calls=0;const raw:any={getObject:async()=>{calls++;return {}},delete:async()=>{calls++},list:async()=>{calls++}};
 const s=scopedGcStore(raw);
 await expect(s.getObject("96bulls/org/claude/session/manifest.json" as any)).rejects.toMatchObject({code:"store_scope"});
 await expect(s.delete("anything" as any)).rejects.toMatchObject({code:"store_operation_refused"});
 await expect(s.list("anything" as any)).rejects.toMatchObject({code:"store_operation_refused"});
 expect(calls).toBe(0);expect(bufferedKeyAllowed("96bulls/../buffered-batches/v1/ws=x/m=srv_"+"a".repeat(32)+"/b="+"b".repeat(32)+"-"+"c".repeat(16)+"-"+"d".repeat(32)+".staged.ndjson.zst")).toBe(false);
});
