import {createSemanticClickHouseClient} from "../src/search-features/semantic-copy-ch.ts";
/** Actual owned PG17 cancellation proof. Run after proof-semantic-copy-recovery.ts.
 * No production URL/credential fallback; only its fixed disposable fixture. */
import pg from "pg";import {execFileSync} from "node:child_process";import {writeFileSync} from "node:fs";
import {SemanticPostgresExecutor,scopeSemanticSql} from "../src/search-features/semantic-copy-postgres.ts";
import {runSemanticCopyTick} from "../src/search-features/semantic-copy-job.ts";
import {runSemanticCopyPage} from "../src/search-features/semantic-copy-runtime.ts";
import {SemanticCopyStore} from "../src/search-features/semantic-copy-store.ts";
import {createClickHouseClient} from "../src/session-pipeline/vendor/search-index/clickhouse.ts";
import {FEATURE_SOURCE_AUTHORITY} from "../src/search-features/source-authority.ts";
import {verifyFeatureSourceAuthority} from "../src/search-features/source.ts";
import {eventFeatureSchemaStatements} from "../src/search-features/event-schema.ts";
import {briefFeatureSchemaStatements} from "../src/search-features/brief-schema.ts";
import {semanticCopySchemaStatements} from "../src/search-features/semantic-copy-schema.ts";
import {semanticSha256,semanticTargetHashInput} from "../src/search-features/semantic-copy-protocol.ts";
const port=Number(execFileSync("docker",["port","semantic-recovery-pg17-20261104","5432"],{encoding:"utf8"}).trim().split(":").at(-1)),chPort=execFileSync("docker",["port","semantic-copy-proof-ch26-20261104","8123"],{encoding:"utf8"}).trim().split(":").at(-1);
const pool=(database:string,user:string,name:string)=>new pg.Pool({host:"127.0.0.1",port,database,user,application_name:name,max:2,connectionTimeoutMillis:1000});
const owner=pool("repair_p1_metadata","postgres","deadline-proof-owner"),sourcePool=pool("repair_p1_source","repair_p1_source_reader","deadline-proof-source"),metadataPool=pool("repair_p1_metadata","service_role","deadline-proof-metadata");
const source=new SemanticPostgresExecutor(sourcePool,true),db=new SemanticPostgresExecutor(metadataPool),endpoint="http://127.0.0.1:"+chPort,config={url:endpoint,user:"proof",password:"synthetic-semantic-only",database:"repair_p2_deadline_target"};
const checks:any[]=[],assert=(v:unknown,message:string)=>{if(!v)throw Error(message)};
try{
 await owner.query("CREATE SEQUENCE sessions.deadline_entered;CREATE FUNCTION sessions.stall_deadline_register() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('sessions.deadline_entered');PERFORM pg_sleep(10);RETURN NEW;END $$;CREATE TRIGGER stall_deadline_register BEFORE INSERT ON sessions.search_feature_sources FOR EACH ROW EXECUTE FUNCTION sessions.stall_deadline_register()");
 const start=performance.now();let error="none";try{await runSemanticCopyTick({db,source,ch:createSemanticClickHouseClient(config),sourceId:"deadline-fresh",endpoint},{maxMs:1000})}catch(e:any){error=e.code??e.reason??e.name}
 const elapsedMs=performance.now()-start,entered=(await owner.query("SELECT is_called FROM sessions.deadline_entered")).rows[0].is_called,bindings=(await owner.query("SELECT count(*)::int n FROM sessions.search_feature_sources WHERE source_id='deadline-fresh'")).rows[0].n;
 const active=(await owner.query("SELECT count(*)::int n FROM pg_stat_activity WHERE application_name='deadline-proof-metadata' AND state='active'")).rows[0].n;
 assert(entered&&elapsedMs<2000&&bindings===0&&active===0&&error!=="none","register deadline/cancellation failed");
 checks.push({name:"actual_register_cancel",budgetMs:1000,elapsedMs,error,entered,noLateBinding:bindings===0,noActiveRpc:active===0});
 await owner.query("DROP TRIGGER stall_deadline_register ON sessions.search_feature_sources;DROP FUNCTION sessions.stall_deadline_register();DROP SEQUENCE sessions.deadline_entered");
 const rootCh=createClickHouseClient({...config,database:"default"});await rootCh.command("DROP DATABASE IF EXISTS repair_p2_heartbeat");await rootCh.command("CREATE DATABASE repair_p2_heartbeat");
 const ch=createClickHouseClient({...config,database:"repair_p2_heartbeat"});for(const sql of [...eventFeatureSchemaStatements(),...briefFeatureSchemaStatements(),...semanticCopySchemaStatements()])await ch.command(sql);
 const parent=(await owner.query("SELECT * FROM sessions.search_feature_sources WHERE source_id='repair-p1-source'")).rows[0],target=await semanticSha256(semanticTargetHashInput(endpoint,ch.database,await semanticSha256(JSON.stringify(["event-feature-projection-v1",...eventFeatureSchemaStatements()])),await semanticSha256(JSON.stringify(["brief-feature-projection-v1",...briefFeatureSchemaStatements()]))));
 const controller=new AbortController(),deadline=Date.now()+5000,timer=setTimeout(()=>controller.abort(),5000),scoped=scopeSemanticSql(db,{signal:controller.signal,deadline}),store=new SemanticCopyStore(scoped,"heartbeat-proof",FEATURE_SOURCE_AUTHORITY.identitySha256);
 await store.register(target,parent.profile_hash,parent.profile_json);
 const at=performance.now();let result;
 try{result=await runSemanticCopyPage({store,source,ch,featureTables:{event:"session_event_features",brief:"session_brief_features"},signal:controller.signal,heartbeatMs:100,verifySource:()=>verifyFeatureSourceAuthority(source,FEATURE_SOURCE_AUTHORITY),
 copyPage:async(_kind,p)=>{
 await owner.query("CREATE SEQUENCE sessions.heartbeat_entered;CREATE FUNCTION sessions.stall_deadline_heartbeat() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.lease_expires_at IS NOT NULL AND OLD.lease_expires_at IS NOT NULL AND NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at AND NEW.lease_nonce IS NOT DISTINCT FROM OLD.lease_nonce AND NEW.generation=OLD.generation AND NEW.state=OLD.state THEN PERFORM nextval('sessions.heartbeat_entered');PERFORM pg_sleep(10);END IF;RETURN NEW;END $$;CREATE TRIGGER stall_deadline_heartbeat BEFORE UPDATE ON sessions.search_feature_copy_state FOR EACH ROW EXECUTE FUNCTION sessions.stall_deadline_heartbeat()");
 await new Promise(r=>setTimeout(r,350));return{published:0,complete:false,next:{...p,phase:"journal",baselineCursor:null,polled:false}};
 }},"event")}finally{clearTimeout(timer);controller.abort()}
 const duration=performance.now()-at,witness=(await owner.query("SELECT is_called FROM sessions.heartbeat_entered")).rows[0].is_called,rows=(await owner.query("SELECT to_jsonb(s) v FROM sessions.search_feature_copy_state s WHERE source_id='heartbeat-proof'")).rows;
 const pending=(await owner.query("SELECT count(*)::int n FROM pg_stat_activity WHERE application_name='deadline-proof-metadata' AND state='active'")).rows[0].n;
 assert(witness&&duration<2000&&result?.status==="published"&&rows[0].v.lease_nonce===null&&pending===0,"heartbeat cancellation/drain failed");
 await new Promise(r=>setTimeout(r,250));assert(JSON.stringify(rows)===JSON.stringify((await owner.query("SELECT to_jsonb(s) v FROM sessions.search_feature_copy_state s WHERE source_id='heartbeat-proof'")).rows),"late heartbeat changed finished state");
 checks.push({name:"actual_inflight_heartbeat_drain",elapsedMs:duration,entered:witness,result,noActiveRpc:pending===0,noLateWrite:true});
 await owner.query("DROP TRIGGER stall_deadline_heartbeat ON sessions.search_feature_copy_state;DROP FUNCTION sessions.stall_deadline_heartbeat();DROP SEQUENCE sessions.heartbeat_entered");
 const report={head:execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim(),node:process.version,checks};writeFileSync("/home/khaliqgant/relay-briefs/semantic-copy-recovery-p2-real-20261104.json",JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
}finally{
 await owner.query("DROP TRIGGER IF EXISTS stall_deadline_register ON sessions.search_feature_sources;DROP FUNCTION IF EXISTS sessions.stall_deadline_register();DROP SEQUENCE IF EXISTS sessions.deadline_entered;DROP TRIGGER IF EXISTS stall_deadline_heartbeat ON sessions.search_feature_copy_state;DROP FUNCTION IF EXISTS sessions.stall_deadline_heartbeat();DROP SEQUENCE IF EXISTS sessions.heartbeat_entered");
 await Promise.all([owner.end(),sourcePool.end(),metadataPool.end()]);
}
