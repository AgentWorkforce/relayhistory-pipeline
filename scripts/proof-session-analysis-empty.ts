/** Synthetic actual SELECT-only source -> leased Node publication empty cases. */
import {mkdir,readdir,writeFile} from "node:fs/promises";import {join,resolve} from "node:path";import {execFileSync} from "node:child_process";
import {legacyAnalysisOracle} from "../src/session-pipeline/__tests__/analysis-oracle-fixture.ts";
import {replayDatabase,addReplayPointer,createFsStore,REPLAY_FIXTURE_IDENTITY as identity} from "../src/session-pipeline/__tests__/replay-fixture.ts";
import {projectSessionDirectory} from "../src/hoopdb/session-directory-projector.ts";import {analysisRefreshTick} from "../src/session-pipeline/analysis-refresh.ts";
import {analysisSnapshotJson} from "../src/session-read-api/session-analysis-artifact.ts";import {artifactDigest} from "../src/session-pipeline/replay-pages.ts";
const arg=process.argv.indexOf("--out");if(arg<0||!process.argv[arg+1])throw Error("--out required");const out=resolve(process.argv[arg+1]!);await mkdir(out,{recursive:true});if((await readdir(out)).length)throw Error("output must be empty");
const cases=[];
for(const label of ["empty_ok","request_only","not_found"]){
 const root=join(out,label);await mkdir(root);const legacy=await legacyAnalysisOracle(),target=await replayDatabase(),store=createFsStore({root:join(root,"objects")});
 try{
  for(const table of ["convergence_events","conversation_turns","session_analysis_requests"])await legacy.pg.query("UPDATE sessions."+table+" SET org_id=$1,workspace_id=$2,session_id=$3",[identity.orgId,identity.workspaceId,identity.sessionId]);
  await legacy.pg.query("UPDATE sessions.session_analysis_jobs SET org_id=$1,session_id=$2,source=$3::jsonb",[identity.orgId,identity.sessionId,JSON.stringify({evidenceWorkspaceIds:[identity.workspaceId]})]);
  await legacy.pg.exec("UPDATE sessions.session_analysis_requests SET analysis_job_id=NULL,brief_job_id=NULL,status='queued',last_error=NULL;DELETE FROM sessions.session_analysis_jobs");
  if(label!=="request_only")await legacy.pg.exec("DELETE FROM sessions.session_analysis_requests");
  if(label==="not_found")await legacy.pg.exec("DELETE FROM sessions.convergence_events;DELETE FROM sessions.conversation_turns");
  const payload=(await legacy.pg.query<any>("SELECT sessions.read_session_analysis_v1($1,$2) p",[identity.orgId,identity.sessionId])).rows[0]!.p;
  await legacy.pg.exec("CREATE ROLE proof_source_reader NOINHERIT;GRANT USAGE ON SCHEMA sessions TO proof_source_reader;GRANT SELECT ON ALL TABLES IN SCHEMA sessions TO proof_source_reader;REVOKE EXECUTE ON FUNCTION sessions.read_session_analysis_v1(text,text),sessions.session_requests_v1_projection(text,text) FROM PUBLIC");
  const source={snapshot:<T>(work:any)=>legacy.pg.transaction(async tx=>{await tx.exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;SET LOCAL ROLE proof_source_reader");return work({query:(sql:string,params?:unknown[])=>tx.query(sql,params)})}) as Promise<T>};
  await addReplayPointer(target.db,{key:"96bulls/org_replay/v1/ws=ws_replay/m=m/h=claude/d=2026-11-01/s=physical/manifest.json",digest:"a".repeat(64),physicalSession:"physical",physicalMachine:"m",canonicalMachine:"m"});
  await projectSessionDirectory(target.db,{orgId:identity.orgId,workspaceId:identity.workspaceId,harness:identity.source,sessionId:"physical"});
  await target.db.query("SELECT sessions.enqueue_session_analysis_cache($1,$2,$3,$4)",[identity.orgId,identity.workspaceId,identity.source,identity.sessionId]);
  const result=await analysisRefreshTick({db:target.db,store,source},{maxJobs:1});if(result.published!==1||result.failed||result.lost)throw Error("publication incomplete");
  await target.pg.exec("INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES('https://replay.invalid','replay-test');BEGIN;SET LOCAL ROLE session_directory_search_worker");
  const now=Math.floor(Date.now()/1000);await target.pg.query("SELECT set_config('request.jwt.claims',$1,true)",[JSON.stringify({role:"session_directory_search_worker",iss:"https://replay.invalid",aud:"replay-test",iat:now,exp:now+30,app_metadata:{org_id:identity.orgId,workspace_ids:[identity.workspaceId]}})]);
  const claim=(await target.pg.query<any>("SELECT sessions.session_directory_analysis_cache($1,$2,$3) c",[identity.workspaceId,identity.source,identity.sessionId])).rows[0]!.c;await target.pg.exec("COMMIT");
  if(!claim.current||claim.snapshotSha256!==artifactDigest(analysisSnapshotJson(payload)))throw Error("source/oracle mismatch");
  const proof={label,head:execFileSync("git",["rev-parse","HEAD"],{encoding:"utf8"}).trim(),runtime:process.version,claim,expected:payload,expectedSha256:claim.snapshotSha256};
  for(const [name,value] of [["claim",claim],["expected-payload",payload],["proof",proof]])await writeFile(join(root,name+".json"),JSON.stringify(value,null,2)+"\n");
  cases.push({label,path:root,head:proof.head,snapshotAt:claim.snapshotAt,expected:payload,expectedSha256:claim.snapshotSha256});
 }finally{await legacy.pg.close();await target.pg.close()}
}
await writeFile(join(out,"cases.json"),JSON.stringify(cases,null,2)+"\n");console.log(JSON.stringify(cases));
