import {createSemanticClickHouseClient} from "../src/search-features/semantic-copy-ch.ts";
import {SemanticPostgresExecutor} from "../src/search-features/semantic-copy-postgres.ts";
/** Independent adversarial state-recovery review fixture; own PG+CH namespaces only. */
import pg from 'pg';import {execFileSync}from'node:child_process';import{readFileSync,writeFileSync}from'node:fs';
import{runSemanticCopyTick}from'../src/search-features/semantic-copy-job.ts';
import{FEATURE_SOURCE_AUTHORITY}from'../src/search-features/source-authority.ts';
import{createClickHouseClient}from'../src/session-pipeline/vendor/search-index/clickhouse.ts';
import{eventFeatureSchemaStatements}from'../src/search-features/event-schema.ts';
import{briefFeatureSchemaStatements}from'../src/search-features/brief-schema.ts';
import{semanticCopySchemaStatements}from'../src/search-features/semantic-copy-schema.ts';
import{readSemanticFeatureAuthority,assertSemanticAuthorityPair}from'../src/search-features/semantic-copy-readiness.ts';
const port=Number(execFileSync('docker',['port','semantic-recovery-pg17-20261104','5432'],{encoding:'utf8'}).trim().split(':').at(-1));
const admin=new pg.Client({host:'127.0.0.1',port,user:'postgres',database:'postgres'});await admin.connect();
const connect=async(database:string,user='postgres')=>{const c=new pg.Pool({host:'127.0.0.1',port,user,database,max:2,connectionTimeoutMillis:1000});return c;};
const rhc='/home/khaliqgant/Projects/AgentWorkforce/relayhistory-cloud-worktrees/ch-brief-search-20261103';
const result:any={head:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),node:process.version,api:FEATURE_SOURCE_AUTHORITY.identitySha256};
let owner:pg.Pool|undefined,meta:pg.Pool|undefined,reader:pg.Pool|undefined,backend:pg.Pool|undefined;
try{
 for(const d of ['repair_p1_source','repair_p1_metadata'])await admin.query('DROP DATABASE IF EXISTS '+d+' WITH(FORCE)');await admin.query('DROP ROLE IF EXISTS repair_p1_source_reader;DROP ROLE IF EXISTS anon;DROP ROLE IF EXISTS authenticated;DROP ROLE IF EXISTS service_role');
 for(const d of ['repair_p1_source','repair_p1_metadata'])await admin.query("CREATE DATABASE "+d+" TEMPLATE template0 LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");
 await admin.query('CREATE ROLE repair_p1_source_reader LOGIN NOINHERIT;CREATE ROLE anon;CREATE ROLE authenticated;CREATE ROLE service_role LOGIN NOINHERIT');
 owner=await connect('repair_p1_source');meta=await connect('repair_p1_metadata');reader=await connect('repair_p1_source','repair_p1_source_reader');backend=await connect('repair_p1_metadata','service_role');
 await owner.query('CREATE EXTENSION vector;CREATE SCHEMA sessions');
 const schema=execFileSync('git',['show','79c887828bbc8767e3f2f1c721c5d06fc5bf6b54:scripts/proof-event-features-real.ts'],{encoding:'utf8'}).match(/("CREATE TABLE sessions\.convergence_events[\s\S]*?");?,?\n\s*\);/u)?.[1];if(!schema)throw Error('fixture schema literal missing');await owner.query(JSON.parse(schema));
 const{readMigrations}=await import(rhc+'/packages/relayhistory/scripts/apply-neon-migration.mjs');const m=readMigrations().find((x:any)=>x.name==='0076_search_feature_journal.sql');if(!m)throw Error('0076 absent');for(const s of m.statements)await owner.query(s);
 await owner.query('CREATE TABLE sessions.__migrations(name text PRIMARY KEY,checksum text)');await owner.query("INSERT INTO sessions.__migrations VALUES('0076_search_feature_journal.sql',$1)",[FEATURE_SOURCE_AUTHORITY.migrationSha256]);
 await owner.query('GRANT USAGE ON SCHEMA sessions TO repair_p1_source_reader');for(const r of FEATURE_SOURCE_AUTHORITY.catalog.relations)await owner.query(r.identity==='sessions.__migrations'?'GRANT SELECT(name,checksum) ON '+r.identity+' TO repair_p1_source_reader':'GRANT SELECT ON '+r.identity+' TO repair_p1_source_reader');for(const f of FEATURE_SOURCE_AUTHORITY.catalog.functions)if(!f.securityDefiner)await owner.query('GRANT EXECUTE ON FUNCTION '+f.identity+' TO repair_p1_source_reader');
 await meta.query('CREATE SCHEMA sessions;GRANT USAGE ON SCHEMA sessions TO service_role');await meta.query(readFileSync('supabase/migrations/20261103100000_semantic_copy_state.sql','utf8'));
 const config={url:'http://127.0.0.1:'+execFileSync('docker',['port','semantic-copy-proof-ch26-20261104','8123'],{encoding:'utf8'}).trim().split(':').at(-1),database:'repair_p1_actual_recovery',user:'proof',password:'synthetic-semantic-only'};const system=createClickHouseClient({...config,database:'default'});await system.command('DROP DATABASE IF EXISTS repair_p1_actual_recovery');await system.command('CREATE DATABASE repair_p1_actual_recovery');const ch=createSemanticClickHouseClient(config);for(const s of [...eventFeatureSchemaStatements(),...briefFeatureSchemaStatements(),...semanticCopySchemaStatements()])await ch.command(s);
 await owner.query("INSERT INTO sessions.convergence_events(org_id,workspace_id,machine_id,user_id,source,session_id,kind,event_id,content,project_id,task_title,ts,ingested_at)VALUES('org','ws','m','u','codex','erased-session','prompt','e','old erased text','p','old erased title','2026-11-01T00:00:00.123456Z',now())");
 const source=new SemanticPostgresExecutor(reader!,true),db=new SemanticPostgresExecutor(backend!);const deps={source,db,ch,sourceId:'repair-p1-source',endpoint:config.url};
 const initial=await runSemanticCopyTick(deps,{maxPages:20,batch:20});if(initial.failed||!initial.eventComplete||!initial.briefComplete)throw Error('initial baseline not ready '+JSON.stringify(initial));
 const oldAuthority=await readSemanticFeatureAuthority(ch,deps.sourceId,'event',FEATURE_SOURCE_AUTHORITY.identitySha256);
 await owner.query("DELETE FROM sessions.convergence_events WHERE event_id='e'");const sourceCount=(await owner.query('SELECT count(*) n FROM sessions.convergence_events')).rows[0].n;
 // A lost/restored metadata checkpoint cannot be allowed to restart baseline over
 // existing feature tables just because the immutable profile binding matches.
 await meta.query("DELETE FROM sessions.search_feature_copy_state WHERE kind='event'");
 const attempts=[];let event:any,brief:any;for(let i=0;i<6;i++){await meta.query('UPDATE sessions.search_feature_copy_state SET next_at=clock_timestamp()');attempts.push(await runSemanticCopyTick(deps,{maxPages:20,batch:20}));try{event=await readSemanticFeatureAuthority(ch,deps.sourceId,'event',FEATURE_SOURCE_AUTHORITY.identitySha256);brief=await readSemanticFeatureAuthority(ch,deps.sourceId,'brief',FEATURE_SOURCE_AUTHORITY.identitySha256);assertSemanticAuthorityPair(event,brief);if(event.checkpoint!=='1')break;}catch{}}
 const stale=await ch.select<any>('SELECT session_id,deleted,content FROM session_event_features FINAL WHERE deleted=0');
 result.initial=initial;result.oldAuthority={checkpoint:oldAuthority.checkpoint,generation:oldAuthority.generation};result.sourceCurrentRows=sourceCount;result.sourceClock=(await owner.query('SELECT revision::text FROM sessions.search_feature_clock')).rows[0].revision;result.attempts=attempts;result.finalPair={event:{complete:event?.complete,checkpoint:event?.checkpoint,generation:event?.generation},brief:{complete:brief?.complete,checkpoint:brief?.checkpoint,generation:brief?.generation}};result.staleLiveFeatures=stale;result.reproduced=sourceCount==='0'&&event?.complete===true&&brief?.complete===true&&event?.checkpoint===result.sourceClock&&stale.length===1;
 let pairAccepted=false;try{assertSemanticAuthorityPair(event,brief);pairAccepted=true}catch{}
 result.repaired=!result.reproduced&&!pairAccepted&&event?.checkpoint===oldAuthority.checkpoint&&attempts.every(a=>a.failed===1);
 if(!result.repaired)throw Error('lost checkpoint was not permanently refused');
 await ch.command('TRUNCATE TABLE session_event_features');
 const authorityOnlyAttempts=[];for(let n=0;n<4;n++){await meta.query('UPDATE sessions.search_feature_copy_state SET next_at=clock_timestamp()');authorityOnlyAttempts.push(await runSemanticCopyTick(deps,{maxPages:20,batch:20}));}
 if(!authorityOnlyAttempts.every(a=>a.failed===1))throw Error('authority-only target was adopted');
 result.authorityOnlyAttempts=authorityOnlyAttempts;
 result.uninitializedH=(await meta.query("SELECT state->'baselineH' h FROM sessions.search_feature_copy_state WHERE kind='event'")).rows[0].h;
 if(result.uninitializedH!==null)throw Error('unowned target captured a replacement H');
 console.log(JSON.stringify(result,null,2));
}finally{await Promise.allSettled([owner?.end(),meta?.end(),reader?.end(),backend?.end(),admin.end()]);writeFileSync('/home/khaliqgant/relay-briefs/semantic-copy-recovery-p1-real-20261104.json',JSON.stringify(result,null,2)+'\n');}
