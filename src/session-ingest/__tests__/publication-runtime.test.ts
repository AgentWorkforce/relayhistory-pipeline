import{test,expect}from'bun:test';
import{publicationEnabled,runCanonicalPublicationRuntime,publicationRuntimeConfig,scopedPublicationStore}from'../publication-runtime.ts';
import type{ObjectStore}from'../../../packages/object-store/src/index.ts';
// Synthetic deployment identity (AWS documentation account, placeholder Supabase ref and instance).
const ACCOUNT='111122223333',REF='abcdefghijklmnopqrst',INSTANCE='i-0123456789abcdef0';
const codeOf=(f:()=>unknown)=>{try{f()}catch(e){return(e as{code?:string}).code}return'no_throw'};
test('publish and erasure have separate default-off gates; disabled does no config/credential/DB/store work',async()=>{
 let calls=0;const fake:any={assets:()=>{calls++;throw Error('unexpected_assets')}};
 expect(await runCanonicalPublicationRuntime({},'publish',fake)).toEqual({enabled:false});expect(await runCanonicalPublicationRuntime({},'gc',fake)).toEqual({enabled:false});expect(calls).toBe(0);
 expect(publicationEnabled({INGEST_PUBLICATION_GC_ENABLED:'true'},'gc')).toBe(true);expect(publicationEnabled({INGEST_PUBLICATION_GC_ENABLED:'true'},'publish')).toBe(false);
 expect(()=>publicationEnabled({INGEST_PUBLISH_ENABLED:'1'},'publish')).toThrow('publication_enabled_invalid');
});
test('runtime refuses wrong compute/account/region/source generation inputs before any factories',()=>{
 expect(()=>publicationRuntimeConfig({AWS_REGION:'us-west-2'})).toThrow('publication_target_required');
 const good={AWS_REGION:'us-east-1',HOOPDB_DB_MODE:'supabase',INGEST_PUBLISH_BUCKET:'owned-bucket',INGEST_PUBLISH_EXPECT_GENERATION:'a'.repeat(64),INGEST_PUBLISH_AWS_ACCOUNT_ID:ACCOUNT,INGEST_PUBLISH_SUPABASE_PROJECT_REF:REF,INGEST_PUBLISH_INSTANCE_ID:INSTANCE,INGEST_PUBLISH_SECRET_ARN:`arn:aws:secretsmanager:us-east-1:${ACCOUNT}:secret:96bulls/test/canonical-publisher-abcdef`,INGEST_PUBLISH_KMS_KEY_ARN:`arn:aws:kms:us-east-1:${ACCOUNT}:key/12345678-1234-1234-1234-123456789abc`,INGEST_PUBLISH_INSTANCE_PROFILE_ARN:`arn:aws:iam::${ACCOUNT}:instance-profile/owned-profile`,INGEST_PUBLISH_ROLE_NAME:'owned-role'};
 expect(publicationRuntimeConfig(good).candidateLimit).toBe(1);expect(publicationRuntimeConfig(good).budgetMs).toBe(1200000);
 expect(()=>publicationRuntimeConfig({...good,INGEST_PUBLISH_EXPECT_GENERATION:'bad'})).toThrow('publication_target_invalid');
 expect(()=>publicationRuntimeConfig({...good,INGEST_PUBLISH_BUDGET_MS:'3600001'})).toThrow('publication_budget_invalid');
 // Deployment identity: required, no default, validated, and every ARN pinned to the configured account.
 const c=publicationRuntimeConfig(good);expect([c.accountId,c.supabaseProjectRef,c.instanceId]).toEqual([ACCOUNT,REF,INSTANCE]);
 for(const name of['INGEST_PUBLISH_AWS_ACCOUNT_ID','INGEST_PUBLISH_SUPABASE_PROJECT_REF','INGEST_PUBLISH_INSTANCE_ID']){
  const{[name]:_,...rest}=good as Record<string,string>;
  expect(codeOf(()=>publicationRuntimeConfig(rest))).toBe('publication_config_required');
  expect(codeOf(()=>publicationRuntimeConfig({...good,[name]:''}))).toBe('publication_config_required');
 }
 for(const patch of[{INGEST_PUBLISH_AWS_ACCOUNT_ID:'12345'},{INGEST_PUBLISH_AWS_ACCOUNT_ID:'[0-9]{12}'},{INGEST_PUBLISH_SUPABASE_PROJECT_REF:'Abcdefghijklmnopqrst'},{INGEST_PUBLISH_INSTANCE_ID:'i-xyz'},{INGEST_PUBLISH_INSTANCE_ID:'0123456789abcdef0'}])
  expect(codeOf(()=>publicationRuntimeConfig({...good,...patch}))).toBe('publication_target_invalid');
 const other='444455556666';
 expect(codeOf(()=>publicationRuntimeConfig({...good,INGEST_PUBLISH_AWS_ACCOUNT_ID:other}))).toBe('publication_target_invalid');
 for(const arn of['INGEST_PUBLISH_SECRET_ARN','INGEST_PUBLISH_KMS_KEY_ARN','INGEST_PUBLISH_INSTANCE_PROFILE_ARN'])
  expect(codeOf(()=>publicationRuntimeConfig({...good,[arn]:(good as Record<string,string>)[arn]!.replace(ACCOUNT,other)}))).toBe('publication_target_invalid');
});
test('runtime uses configured identity for IMDS credentials and the database login',async()=>{
 const good={AWS_REGION:'us-east-1',HOOPDB_DB_MODE:'supabase',INGEST_PUBLISH_ENABLED:'true',INGEST_PUBLISH_BUCKET:'owned-bucket',INGEST_PUBLISH_EXPECT_GENERATION:'a'.repeat(64),INGEST_PUBLISH_AWS_ACCOUNT_ID:ACCOUNT,INGEST_PUBLISH_SUPABASE_PROJECT_REF:REF,INGEST_PUBLISH_INSTANCE_ID:INSTANCE,INGEST_PUBLISH_SECRET_ARN:`arn:aws:secretsmanager:us-east-1:${ACCOUNT}:secret:96bulls/test/canonical-publisher-abcdef`,INGEST_PUBLISH_KMS_KEY_ARN:`arn:aws:kms:us-east-1:${ACCOUNT}:key/12345678-1234-1234-1234-123456789abc`,INGEST_PUBLISH_INSTANCE_PROFILE_ARN:`arn:aws:iam::${ACCOUNT}:instance-profile/owned-profile`,INGEST_PUBLISH_ROLE_NAME:'owned-role'};
 const assets=async()=>({expected:{names:['x'],generation:'a'.repeat(64)},verifiers:{searchWorker:'x',tokenVerifier:'x',ingestSeeder:'x',ingest:'x'}});
 let poolUser='';
 await expect(runCanonicalPublicationRuntime(good,'publish',{assets,secret:async()=>JSON.stringify({hoopdb_url:`postgresql://bulls96_session_ingest.${REF}:synthetic-unused@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require`}),
  pool:c=>{poolUser=String(c.user);throw Object.assign(Error('stop'),{code:'stop_after_pool_config'})}})).rejects.toMatchObject({code:'stop_after_pool_config'});
 expect(poolUser).toBe(`bulls96_session_ingest.${REF}`);
 // A login for a different project ref is refused even when everything else matches.
 await expect(runCanonicalPublicationRuntime({...good,INGEST_PUBLISH_SUPABASE_PROJECT_REF:'zyxwvutsrqponmlkjihg'},'publish',{assets,secret:async()=>JSON.stringify({hoopdb_url:`postgresql://bulls96_session_ingest.${REF}:synthetic-unused@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require`}),
  pool:()=>{throw Error('pool_must_not_be_built')}})).rejects.toMatchObject({code:'database_target'});
 // IMDS identity document must carry the configured account and instance (no hardcoded host).
 const imds=(account:string,instance:string)=>(async(input:Parameters<typeof fetch>[0])=>{const u=String(input);
  if(u.endsWith('/latest/api/token'))return new Response('token');
  if(u.endsWith('/latest/dynamic/instance-identity/document'))return new Response(JSON.stringify({accountId:account,instanceId:instance,region:'us-east-1'}));
  return new Response('',{status:404});})as typeof fetch;
 const failure=(fetcher:typeof fetch)=>runCanonicalPublicationRuntime(good,'publish',{assets,fetcher}).then(()=>'resolved',(e:{code?:string})=>e.code);
 expect(await failure(imds('999988887777',INSTANCE))).toBe('managed_imds_instance_identity');
 expect(await failure(imds(ACCOUNT,'i-0fedcba9876543210'))).toBe('managed_imds_instance_identity');
 // The configured account and instance pass the identity check and reach the next IMDS step.
 expect(await failure(imds(ACCOUNT,INSTANCE))).toBe('managed_imds_http_404');
});
test('runtime may read legacy bases but only PUT generation-qualified keys; arbitrary erase/list/presign refused',async()=>{
 let calls=0;const raw=new Proxy({backend:'s3'},{get(t,k){if(k==='backend')return t.backend;return async()=>{calls++;return{}}}})as unknown as ObjectStore;
 const store=scopedPublicationStore(raw),base='96bulls/org/v1/ws=w/m=srv_x/h=claude/d=2026/11/01/s=s/manifest.json',gen='96bulls/org/upload-generations/g=12345678-1234-1234-1234-123456789abc/v1/ws=w/m=srv_x/h=claude/d=2026/11/01/s=s/publications/abcdefab-1234-1234-1234-123456789abc/manifest.json';
 await store.get(base as never);await store.put(gen as never,new Uint8Array([1]));expect(calls).toBe(2);
 for(const run of[()=>store.put(base as never,new Uint8Array([1])),()=>store.delete(gen as never),()=>store.list('96bulls' as never),()=>store.presignGet(gen as never,10)])await expect(run()).rejects.toThrow('publication_');expect(calls).toBe(2);
});
