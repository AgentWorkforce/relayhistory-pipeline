/** Portable (self-host) canonical publication runtime: INGEST_PUBLICATION_TARGET=portable
 * (publication-target.ts). Same publish/GC job, RPCs, store scope, scrub and
 * create-only object protocol as the hosted runtime (publication-runtime.ts); only
 * the target bindings differ: an S3-compatible endpoint (MinIO) with static keys
 * instead of IMDSv2 + KMS, and an operator-supplied Postgres URL with an explicit
 * TLS profile instead of Secrets Manager + the pinned Supabase pooler.
 *
 * Default off: the same INGEST_PUBLISH_ENABLED / INGEST_PUBLICATION_GC_ENABLED gates.
 * Enabled, every value below is required unless marked optional; nothing has a
 * default target and nothing falls back to ambient AWS/PG* environment:
 *
 *   INGEST_PUBLISH_BUCKET              bucket name (same rule as hosted)
 *   INGEST_PUBLISH_EXPECT_GENERATION   compiled schema generation (64 hex)
 *   INGEST_PUBLISH_S3_ENDPOINT         bare origin; https, loopback http, or
 *                                      http://<svc>.<ns>.svc[.cluster.local] with
 *                                      ALLOW_IN_CLUSTER_HTTP=true (destination-url.ts)
 *   INGEST_PUBLISH_S3_REGION           SigV4 region the store expects; must equal the
 *                                      frontier rows' target_region
 *   INGEST_PUBLISH_S3_ACCESS_KEY_ID[_FILE]       exactly one of the pair
 *   INGEST_PUBLISH_S3_SECRET_ACCESS_KEY[_FILE]   exactly one of the pair
 *   INGEST_PUBLISH_S3_SSE              optional: unset | "none" | "AES256"
 *   INGEST_PUBLISH_DATABASE_URL[_FILE] exactly one: postgresql://bulls96_session_ingest:<pw>@host[:port]/db
 *   HOOPDB_DB_MODE                     local | selfhosted | cloudsql | rds | supabase
 *                                      (the verifier profile; no default)
 *   SESSION_POSTGRES_TLS_PROFILE       required explicitly: supabase | custom-ca
 *                                      (+ SESSION_POSTGRES_CA_FILE / _CA_SHA256), postgres-tls.ts
 *   INGEST_PUBLISH_BUDGET_MS / INGEST_PUBLISH_BATCH_LIMIT / INGEST_PUBLICATION_GC_LIMIT  optional, as hosted
 *
 * Any hosted-only identity variable (secret/KMS/instance-profile ARN, role name,
 * account, Supabase ref, instance id) refuses startup: one process never mixes targets.
 * A *_FILE is a mounted Secret file: regular file, 1..64 KiB, one optional trailing
 * newline. Refusal codes never carry a value.
 */
import pg from'pg';
import{readFileSync,statSync}from'node:fs';
import{isAbsolute}from'node:path';
import{createS3Store,type ObjectStore}from'../../packages/object-store/src/index.ts';
import type{AwsCredentials}from'../../packages/object-store/src/sigv4.ts';
import{derivedSchemaAssets,type DerivedSchemaAssets}from'../session-pipeline/derived-schema.ts';
import{createS3Eraser,type PreservedVersionEraser}from'../migration/neon-s3-erasure/eraser.ts';
import{SemanticPostgresExecutor}from'../search-features/semantic-copy-postgres.ts';
import{postgresConnectionString,postgresTlsOptions}from'../shared/postgres-tls.ts';
import{assertDestinationUrl,destinationPolicy}from'../shared/destination-url.ts';
import{publicationEnabled,scopedPublicationStore,publicationGenerationKey,publicationRuntimeConfig,type PublicationRuntimeConfig}from'./publication-runtime.ts';
import{publicationTargetKind}from'./publication-target.ts';
import{ingestLoginIdentity}from'./ingest-login.ts';
import{publicationRpc}from'./publication-rpc.ts';
import{canonicalPublicationTick}from'./publication-job.ts';
import{routedRawChunkSealerDeps}from'../../packages/pipeline/src/lib/scrub-engine/index.ts';
import{publicationGcLedger,collectPublicationObject}from'./publication-gc.ts';
import type{PublicationTarget}from'./publication-protocol.ts';
import{lockerProvider,type LockerProvider}from'../shared/locker-selection.ts';
const no=(code:string):never=>{throw Object.assign(Error('publication_'+code),{code:'publication_'+code});};
const DB_MODES=['local','selfhosted','cloudsql','rds','supabase']as const;
export type PortableDbMode=typeof DB_MODES[number];
/** Hosted-only identity: present in a portable process means a mixed or copied target. */
export const HOSTED_ONLY_VARS=['INGEST_PUBLISH_SECRET_ARN','INGEST_PUBLISH_KMS_KEY_ARN','INGEST_PUBLISH_INSTANCE_PROFILE_ARN','INGEST_PUBLISH_ROLE_NAME',
 'INGEST_PUBLISH_AWS_ACCOUNT_ID','INGEST_PUBLISH_SUPABASE_PROJECT_REF','INGEST_PUBLISH_INSTANCE_ID']as const;
export type SecretSource={env:string}|{file:string};
export interface PortablePublicationConfig{
 kind:'portable';bucket:string;region:string;endpoint:string;keyRoot:'96bulls';generation:string;dbMode:PortableDbMode;
 /** The install's Locker provider, from the same selection the read API uses (LOCKER_PROVIDER, default s3).
  * Stamped on pointer rows and landed messages so canonical reads accept them. */
 provider:LockerProvider;
 sse:'AES256'|undefined;budgetMs:number;candidateLimit:number;gcLimit:number;
 /** Where each secret is read from at run time; never the value. */
 sources:{accessKeyId:SecretSource;secretAccessKey:SecretSource;databaseUrl:SecretSource};
}
function source(env:NodeJS.ProcessEnv,name:string):SecretSource{
 const value=env[name],file=env[name+'_FILE'];
 if(value!==undefined&&file!==undefined)no('secret_source_ambiguous');
 if(value)return{env:name};
 if(file){if(!isAbsolute(file))no('secret_file_invalid');return{file};}
 return no('config_required');
}
export function portablePublicationConfig(env:NodeJS.ProcessEnv):PortablePublicationConfig{
 if(HOSTED_ONLY_VARS.some(k=>env[k]!==undefined))no('target_ambiguous');
 const need=(k:string)=>{if(!env[k])no('config_required');return env[k]!};
 const bucket=need('INGEST_PUBLISH_BUCKET'),generation=need('INGEST_PUBLISH_EXPECT_GENERATION'),rawEndpoint=need('INGEST_PUBLISH_S3_ENDPOINT'),region=need('INGEST_PUBLISH_S3_REGION');
 const mode=need('HOOPDB_DB_MODE');
 if(!env.SESSION_POSTGRES_TLS_PROFILE)no('config_required');
 if(!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket)||!/^[a-f0-9]{64}$/.test(generation)||!/^[a-z0-9-]{1,63}$/.test(region)||!(DB_MODES as readonly string[]).includes(mode))no('target_invalid');
 let endpoint:URL;
 try{endpoint=assertDestinationUrl('INGEST_PUBLISH_S3_ENDPOINT',rawEndpoint,destinationPolicy(env),{allowPath:false});}catch{return no('target_invalid');}
 let provider:LockerProvider;
 try{provider=lockerProvider(env);}catch{return no('target_invalid');}
 const sseRaw=env.INGEST_PUBLISH_S3_SSE;
 if(sseRaw!==undefined&&sseRaw!=='none'&&sseRaw!=='AES256')no('target_invalid');
 // Validated now so a bad trust profile fails startup; re-read when the pool is built.
 try{postgresTlsOptions(env);}catch{no('database_tls');}
 const sources={accessKeyId:source(env,'INGEST_PUBLISH_S3_ACCESS_KEY_ID'),secretAccessKey:source(env,'INGEST_PUBLISH_S3_SECRET_ACCESS_KEY'),databaseUrl:source(env,'INGEST_PUBLISH_DATABASE_URL')};
 const budgetMs=Number(env.INGEST_PUBLISH_BUDGET_MS??'1200000'),candidateLimit=Number(env.INGEST_PUBLISH_BATCH_LIMIT??'1'),gcLimit=Number(env.INGEST_PUBLICATION_GC_LIMIT??'20');
 if(!Number.isSafeInteger(budgetMs)||budgetMs<5000||budgetMs>3600000||!Number.isInteger(candidateLimit)||candidateLimit<1||candidateLimit>20||!Number.isInteger(gcLimit)||gcLimit<1||gcLimit>100)no('budget_invalid');
 return{kind:'portable',bucket,region,endpoint:endpoint.origin,keyRoot:'96bulls',generation,dbMode:mode as PortableDbMode,provider,sse:sseRaw==='AES256'?'AES256':undefined,budgetMs,candidateLimit,gcLimit,sources};
}
/** The selected target's validated config (startup checks such as the session-publication
 * service's budget read): hosted is publicationRuntimeConfig exactly, unchanged. */
export function publicationConfigForTarget(env:NodeJS.ProcessEnv):PublicationRuntimeConfig|PortablePublicationConfig{
 return publicationTargetKind(env)==='portable'?portablePublicationConfig(env):publicationRuntimeConfig(env);
}
export type SecretFileReader=(path:string)=>string;
const MAX_SECRET_FILE=65536;
function defaultReader(path:string):string{
 const st=statSync(path);if(!st.isFile()||st.size<1||st.size>MAX_SECRET_FILE)no('secret_file_invalid');
 return readFileSync(path,'utf8');
}
function readSecret(env:NodeJS.ProcessEnv,src:SecretSource,read:SecretFileReader):string{
 let text:string;
 if('env'in src)text=env[src.env]??'';
 else{try{text=read(src.file);}catch(e){if((e as{code?:string})?.code==='publication_secret_file_invalid')throw e;return no('secret_file_invalid');}
  if(text.length>MAX_SECRET_FILE)no('secret_file_invalid');text=text.replace(/\r?\n$/,'');}
 // eslint-disable-next-line no-control-regex
 if(!text||/[\u0000-\u001f\u007f]/.test(text))no('secret_invalid');
 return text;
}
/** Discrete pool fields from the sanitized URL (no connectionString, so neither URL
 * query options, PG* env nor .pgpass can retarget, disable TLS or supply a login). */
export function portablePoolConfig(url:string,env:NodeJS.ProcessEnv):pg.PoolConfig{
 let target:URL;try{target=new URL(postgresConnectionString(url));}catch{return no('database_target');}
 let user:string,password:string,database:string;
 try{user=decodeURIComponent(target.username);password=decodeURIComponent(target.password);database=decodeURIComponent(target.pathname.replace(/^\//,''));}catch{return no('database_target');}
 if(!target.hostname||!user||!password||!database||database.includes('/')||target.hash||[...target.searchParams.keys()].length)no('database_target');
 if(user!=='bulls96_session_ingest')no('database_role');
 const port=target.port?Number(target.port):5432;
 let ssl;try{ssl=postgresTlsOptions(env);}catch{return no('database_tls');}
 return{host:target.hostname.replace(/^\[(.*)\]$/,'$1'),port,user,password,database,ssl,max:4,connectionTimeoutMillis:1000,idleTimeoutMillis:1000,
  application_name:'96bulls-canonical-publication',options:'-c idle_in_transaction_session_timeout=10s'};
}
export function portablePublicationSecrets(config:PortablePublicationConfig,env:NodeJS.ProcessEnv,read:SecretFileReader=defaultReader):{credentials:AwsCredentials;pool:pg.PoolConfig}{
 const accessKeyId=readSecret(env,config.sources.accessKeyId,read),secretAccessKey=readSecret(env,config.sources.secretAccessKey,read);
 if(!/^[A-Za-z0-9._~+=-]{3,128}$/.test(accessKeyId)||secretAccessKey.length<8||secretAccessKey.length>256||/\s/.test(secretAccessKey))no('secret_invalid');
 return{credentials:{accessKeyId,secretAccessKey},pool:portablePoolConfig(readSecret(env,config.sources.databaseUrl,read),env)};
}
export interface PortablePublicationOverrides{
 assets?:()=>Promise<DerivedSchemaAssets>;readFile?:SecretFileReader;
 pool?:(config:pg.PoolConfig)=>pg.Pool;store?:(config:Parameters<typeof createS3Store>[0])=>ObjectStore;
 eraser?:(config:Parameters<typeof createS3Eraser>[0])=>PreservedVersionEraser;fetcher?:typeof fetch;
}
/** Factories are test-only ports; process entry never reads overrides from input.
 * The body after target binding mirrors runCanonicalPublicationRuntime step for step. */
export async function runPortablePublicationRuntime(env:NodeJS.ProcessEnv,mode:'publish'|'gc',overrides:PortablePublicationOverrides={},external?:AbortSignal){
 if(!publicationEnabled(env,mode))return{enabled:false as const};
 const config=portablePublicationConfig(env),assets=await(overrides.assets??derivedSchemaAssets)();
 if(assets.expected.generation!==config.generation||!assets.expected.names.length||Object.values(assets.verifiers).length!==4||Object.values(assets.verifiers).some(s=>!s?.trim()))no('compiled_generation_mismatch');
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),config.budgetMs),signal=AbortSignal.any([controller.signal,...(external?[external]:[])]),deadline=Date.now()+config.budgetMs;
 let pool:pg.Pool|undefined;
 try{
  const fetcher=(async(input:Parameters<typeof fetch>[0],init?:RequestInit)=>{signal.throwIfAborted();return(overrides.fetcher??fetch)(input,{...init,signal:AbortSignal.any([signal,AbortSignal.timeout(30000),...(init?.signal?[init.signal]:[])])});})as typeof fetch;
  const secrets=portablePublicationSecrets(config,env,overrides.readFile);signal.throwIfAborted();
  pool=(overrides.pool??(c=>new pg.Pool(c)))(secrets.pool);pool.on('error',()=>{});
  const sql=new SemanticPostgresExecutor(pool),read=new SemanticPostgresExecutor(pool,true),context=()=>({signal,deadline:Math.min(deadline,Date.now()+1800)});
  // A genuine login as the ingest role, before any verifier, store or RPC work: not SET ROLE and not a
  // privileged backend's SET SESSION AUTHORIZATION (ingest-login.ts). Refusal is one code, no detail.
  if(await ingestLoginIdentity(q=>read.queryBounded<Record<string,unknown>>(q,[],context())))no('database_role');
  // dbMode is one of DB_MODES (a fixed allowlist), never free text.
  await read.queryBounded(`SET LOCAL hoopdb.db_mode='${config.dbMode}';`+Object.values(assets.verifiers).join('\n'),[],context());
  const target:PublicationTarget={bucket:config.bucket,region:config.region,keyRoot:config.keyRoot,generation:config.generation,provider:config.provider};
  const raw=scopedPublicationStore((overrides.store??createS3Store)({bucket:config.bucket,region:config.region,endpoint:config.endpoint,credentials:secrets.credentials,
   ...(config.sse?{serverSideEncryption:config.sse}:{}),fetch:fetcher}));
  if(mode==='publish')return await canonicalPublicationTick({target,rpc:publicationRpc(sql,target),store:()=>raw,...routedRawChunkSealerDeps()},{enabled:true,signal,maxJobMs:config.budgetMs,candidateLimit:config.candidateLimit});
  const eraser=(overrides.eraser??createS3Eraser)({bucket:config.bucket,region:config.region,endpoint:config.endpoint,credentials:secrets.credentials,fetch:fetcher});
  const ledger=publicationGcLedger(sql,target),tickets=await ledger.claim(config.gcLimit,signal);let finished=0;
  for(const ticket of tickets){signal.throwIfAborted();if(!publicationGenerationKey(ticket.key))no('eraser_scope');
   if(await collectPublicationObject(ticket,{ledger,target,store:raw,eraser:{eraseObjectExcept:async(key,version)=>{if(!publicationGenerationKey(key))no('eraser_scope');signal.throwIfAborted();return eraser.eraseObjectExcept(key,version);}}},signal))finished++;
  }
  signal.throwIfAborted();return{enabled:true as const,examined:tickets.length,finished};
 }finally{controller.abort();clearTimeout(timer);if(pool)await pool.end();}
}
