/** Managed full-summary job, default OFF. No owner/service-role fallback, raw
 * writes, deletes or credential files. Production supplies no factory overrides. */
import pg from"pg";
import{createS3Store,parseSessionKey,publicationDerivedKey,type ObjectStore}from"../../packages/object-store/src/index.ts";
import{MODEL_WORKER_ROLE,MODEL_WORKER_FUNCTIONS}from"../shared/model-worker-contract.ts";
import{sessionsFunctionGateSql}from"../shared/db.ts";
import{ROLE_VERIFIERS}from"../hoopdb/role-verifier-contract.ts";
import{SemanticPostgresExecutor}from"../search-features/semantic-copy-postgres.ts";
import{derivedSchemaAssets,type DerivedSchemaAssets}from"../session-pipeline/derived-schema.ts";
import{SUPABASE_PROD_CA_2021}from"../session-pipeline/supabase-ca.ts";
import{awsJsonCall,lambdaCredentials}from"../session-pipeline/aws-json.ts";
import{masterKeyring,parseMasterKeys}from"../shared/keyring.ts";
import{fullModelConfig,createFullModelTransport}from"./full-model-provider.ts";
import{runFullModelWorkTick}from"./full-model-tick.ts";
function no(code:string):never{throw Object.assign(Error(code),{code})}
export interface ModelJobConfig{region:"us-east-1";bucket:string;kmsKeyArn:string;secretArn:string;generation:string;budgetMs:number}
export function modelJobConfig(env:NodeJS.ProcessEnv):ModelJobConfig{
 if(env.AWS_REGION!=="us-east-1"||env.HOOPDB_DB_MODE!=="supabase")no("model_job_target");
 const bucket=env.PIPELINE_RAW_S3_BUCKET??"",kmsKeyArn=env.SESSION_MODEL_KMS_KEY_ARN??"",secretArn=env.SESSION_MODEL_SECRET_ARN??"",generation=env.SESSION_MODEL_EXPECT_GENERATION??"";
 if(!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket)||!/^arn:aws:kms:us-east-1:941433935742:key\/[0-9a-f-]{36}$/.test(kmsKeyArn)||!/^arn:aws:secretsmanager:us-east-1:941433935742:secret:96bulls\/[A-Za-z0-9_./-]+\/session-model-[A-Za-z0-9]{6}$/.test(secretArn)||!/^[a-f0-9]{64}$/.test(generation))no("model_job_config");
 const budgetMs=Number(env.SESSION_MODEL_BUDGET_MS??"240000");if(!Number.isInteger(budgetMs)||budgetMs<80000||budgetMs>250000)no("model_job_budget");
 return{region:"us-east-1",bucket,kmsKeyArn,secretArn,generation,budgetMs};
}
export function modelPoolConfig(url:string):pg.PoolConfig{
 let u:URL;try{u=new URL(url)}catch{no("model_database_target")}
 if(!["postgres:","postgresql:"].includes(u.protocol)||u.hostname!=="aws-0-us-east-1.pooler.supabase.com"||u.port!=="5432"||u.pathname!=="/postgres"||decodeURIComponent(u.username)!==MODEL_WORKER_ROLE+".pyamkrhoyszgzirqqffq"||!u.password||u.hash)no("model_database_target");
 for(const[k,v]of u.searchParams)if(!((k==="sslmode"&&["require","verify-full"].includes(v))||(k==="channel_binding"&&v==="require")))no("model_database_options");
 return{host:u.hostname,port:5432,database:"postgres",user:decodeURIComponent(u.username),password:decodeURIComponent(u.password),
  ssl:{ca:SUPABASE_PROD_CA_2021,rejectUnauthorized:true,servername:u.hostname},max:1,connectionTimeoutMillis:1000,idleTimeoutMillis:1000,application_name:"96bulls-full-model"};
}
function secretValue(text:string){
 let v:any;try{v=JSON.parse(text)}catch{no("model_secret_invalid")}
 if(!v||typeof v!=="object"||Array.isArray(v)||Object.keys(v).sort().join(",")!=="hoopdb_url,openai_api_key,summary_master_keys"||typeof v.hoopdb_url!=="string"||typeof v.openai_api_key!=="string"||!v.openai_api_key.trim()||typeof v.summary_master_keys!=="string")no("model_secret_invalid");
 const pool=modelPoolConfig(v.hoopdb_url),keys=parseMasterKeys(v.summary_master_keys);
 return{pool,keys,apiKey:v.openai_api_key as string};
}
/** GET graph/slices only, immutable writes to the three derived model families.
 * The source/graph reader and registration SQL enforce exact tenant/current
 * namespace in addition to this deployment boundary. No source manifests/raw. */
export function modelStore(store:ObjectStore):ObjectStore{
 const derived=(key:string)=>{
  const p=parseSessionKey(key),m=p&&/^publications\/([^/]+)\/derived\/(bob-slice|summary-short|summary-long)\/([^/]+)\.bin$/.exec(p.rest);
  if(!p?.prefix?.startsWith("96bulls/")||!m)return false;
  try{return publicationDerivedKey(key.slice(0,key.length-p.rest.length)+"publications/"+m[1]+"/manifest.json",m[3]!,m[2]!)===key}catch{return false}
 };
 const graph=(key:string)=>{const p=parseSessionKey(key);return!!p&&p.prefix?.startsWith("96bulls/")&&/^publications\/[0-9a-f-]{36}\/derived\/replay\//.test(p.rest)};
 return new Proxy(store,{get(target,k){
  const value=Reflect.get(target,k);
  if(typeof value!=="function")return value;
  if(["get","getObject","head"].includes(String(k)))return async(key:string,...rest:unknown[])=>{if(!derived(String(key))&&!graph(String(key)))no("model_store_scope");return Reflect.apply(value,target,[key,...rest])};
  if(k==="put")return async(key:string,body:Uint8Array,options:any)=>{if(!derived(String(key))||options?.ifNoneMatch!==true||body.byteLength>1048576)no("model_store_write_scope");return target.put(key as never,body,options)};
  return async()=>no("model_store_operation");
 }});
}
export interface ModelJobOverrides{
 assets?:()=>Promise<DerivedSchemaAssets>;secret?:(config:ModelJobConfig,fetcher:typeof fetch)=>Promise<string>;
 pool?:(config:pg.PoolConfig)=>pg.Pool;store?:(config:Parameters<typeof createS3Store>[0])=>ObjectStore;
 fetcher?:typeof fetch;providerFetch?:typeof fetch;
}
export async function runFullModelJob(env:NodeJS.ProcessEnv,remainingMs:()=>number,overrides:ModelJobOverrides={}){
 if(env.SESSION_FULL_MODEL_ENABLED!=="true")return{enabled:false,claimed:0,completed:0};
 const modelConfig=fullModelConfig(env);if(!modelConfig)no("model_job_provider_required");
 const config=modelJobConfig(env),assets=await(overrides.assets??derivedSchemaAssets)();
 if(assets.expected.generation!==config.generation||!assets.expected.names.length||Object.keys(assets.verifiers).length!==ROLE_VERIFIERS.length||ROLE_VERIFIERS.some(v=>!assets.verifiers[v.key]?.trim()))no("model_compiled_generation");
 const budget=Math.min(config.budgetMs,remainingMs()-15000);if(budget<80000)no("model_job_deadline");
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(Error("model_job_deadline")),budget),deadline=Date.now()+budget;
 const actualFetch=overrides.fetcher??fetch,boundedFetch=(async(i:Parameters<typeof fetch>[0],init?:RequestInit)=>actualFetch(i,{...init,signal:AbortSignal.any([controller.signal,...(init?.signal?[init.signal]:[])])}))as typeof fetch;
 const pools:pg.Pool[]=[];
 try{
  const credentials=()=>{const c=lambdaCredentials();if(!/^ASIA[A-Z0-9]{16}$/.test(c.accessKeyId)||!c.sessionToken)no("model_managed_credentials");return c};
  const text=overrides.secret?await overrides.secret(config,boundedFetch):await(async()=>{
   const result=await awsJsonCall<{SecretString?:string}>({service:"secretsmanager",target:"secretsmanager.GetSecretValue",region:config.region,body:{SecretId:config.secretArn},credentials:credentials(),fetcher:boundedFetch,timeoutMs:5000});
   if(!result.SecretString)no("model_secret_empty");return result.SecretString;
  })();
  controller.signal.throwIfAborted();
  const secret=secretValue(text),pool=(overrides.pool??(c=>new pg.Pool(c)))(secret.pool);pools.push(pool);
  const heartbeatPool=(overrides.pool??(c=>new pg.Pool(c)))(secret.pool);pools.push(heartbeatPool);
  for(const p of pools)p.on("error",()=>{});
  const db=new SemanticPostgresExecutor(pool),heartbeatDb=new SemanticPostgresExecutor(heartbeatPool),read=new SemanticPostgresExecutor(pool,true);
  const ctx={signal:controller.signal,deadline:Math.min(deadline,Date.now()+10000)};
  const who=(await read.queryBounded<{me:string;session:string}>("SELECT current_user AS me,session_user AS session",[],ctx)).rows[0];
  if(who?.me!==MODEL_WORKER_ROLE||who.session!==MODEL_WORKER_ROLE)no("model_database_role");
  await read.queryBounded("SET LOCAL hoopdb.db_mode='supabase';"+ROLE_VERIFIERS.map(v=>assets.verifiers[v.key]).join("\n")+"\n"+sessionsFunctionGateSql(MODEL_WORKER_FUNCTIONS),[],ctx);
  const store=(overrides.store??createS3Store)({bucket:config.bucket,region:config.region,credentials:async()=>credentials(),serverSideEncryption:"aws:kms",sseKmsKeyId:config.kmsKeyArn,fetch:boundedFetch});
  const keyring=await masterKeyring(secret.keys);
  const model=createFullModelTransport({config:modelConfig,apiKey:()=>secret.apiKey,fetch:overrides.providerFetch??boundedFetch});
  return await runFullModelWorkTick({enabled:true,schemaGeneration:config.generation,db,heartbeatDb,store:modelStore(store),keyring,config:modelConfig,model,remainingMs:()=>Math.min(remainingMs(),deadline-Date.now()),signal:controller.signal,maxSteps:64});
 }finally{controller.abort(Error("model_job_finished"));clearTimeout(timer);await Promise.all(pools.map(p=>p.end()))}
}
