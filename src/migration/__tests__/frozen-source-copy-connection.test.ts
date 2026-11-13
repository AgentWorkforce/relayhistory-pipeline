import {test,expect,afterAll} from "bun:test";
import {fileURLToPath} from "node:url";
import {mkdtempSync,readFileSync,statSync,rmSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {SCHEMA_RELEASE} from "../../hoopdb/schema-release-contract.ts";
import {FROZEN_COPY_TEST_OWNER_PASSWORD,SYNTHETIC_NEON_URL,syntheticOwnerUrl} from "./fixtures/frozen-copy-owner-url.ts";
import {PLAN_RUN,RUN_ID,SOURCE_SHA,fixtureApproval,fixtureFacts,supervisorEnv} from "./fixtures/frozen-copy-supervisor.ts";
import {FROZEN_CUSTODY} from "../frozen-source-copy-supervisor.ts";
const script=fileURLToPath(new URL("../../../scripts/frozen-source-copy.ts",import.meta.url));
const preload=fileURLToPath(new URL("./fixtures/frozen-copy-preload.ts",import.meta.url));
const dir=mkdtempSync(join(tmpdir(),"fsc-receipt-"));
afterAll(()=>rmSync(dir,{recursive:true,force:true}));
interface Opts {fail?:string; undefinedFailure?:boolean; connect?:string; lost?:string; identity?:string; env?:Record<string,string>; doc?:Record<string,unknown>|string; extra?:string[];
 /** default true: run as the protected workflow's job (Actions env + synthetic GitHub + the matching typed approval). */
 supervised?:boolean; approval?:string}
// The typed approval for this run's --page / --max-pages / --budget-ms (what the driver derives against the fixture).
const approvalFor=(extra:string[])=>{
 const v=(k:string,d:number)=>{const i=extra.indexOf(k);return i>=0?Number(extra[i+1]):d;};
 return fixtureApproval(fixtureFacts("copy",{page:v("--page",5000),maxPages:v("--max-pages",0),budgetMs:v("--budget-ms",6*60*60*1000)}));
};
// stdin must be a real pipe: the credential reader refuses a regular file (which is how spawnSync's `input` arrives).
async function run(o:Opts={}) {
 const doc=o.doc??{neonOwnerUrl:SYNTHETIC_NEON_URL,supabaseOwnerUrl:syntheticOwnerUrl()};
 const extra=o.extra??[];
 const sup=o.supervised===false?{}:{...supervisorEnv("copy",o.approval??approvalFor(extra)),FROZEN_COPY_TEST_GITHUB:"1"};
 const child=Bun.spawn([process.execPath,"--preload",preload,script,"--copy","--concurrency","2",...extra],{
  stdin:"pipe",stdout:"pipe",stderr:"pipe",timeout:10000,env:{PATH:process.env.PATH,...sup,...(o.fail?{FROZEN_COPY_TEST_FAIL:o.fail}:{}),...(o.undefinedFailure?{FROZEN_COPY_TEST_UNDEFINED:"1"}:{}),
   ...(o.connect?{FROZEN_COPY_TEST_CONNECT:o.connect}:{}),...(o.lost?{FROZEN_COPY_TEST_LOST:o.lost}:{}),...(o.identity?{FROZEN_COPY_TEST_IDENTITY:o.identity}:{}),...(o.env??{})},
 });
 child.stdin.write((typeof doc==="string"?doc:JSON.stringify(doc))+"\n");
 await child.stdin.end();
 const [stdout,stderr,status]=await Promise.all([new Response(child.stdout).text(),new Response(child.stderr).text(),child.exited]);
 const result={stdout,stderr,status};
 const lines=result.stdout.split("\n").filter(x=>x.startsWith("{")).map(x=>JSON.parse(x));
 const proof=lines.find(x=>x.syntheticCopyProof)?.syntheticCopyProof;
 const receipt=lines.find(x=>x.m==="receipt");
 return {result,proof,receipt};
}
const noSecret=(r:{result:{stdout:string;stderr:string}})=>{
 expect(r.result.stdout).not.toContain(FROZEN_COPY_TEST_OWNER_PASSWORD);
 expect(r.result.stderr).not.toContain(FROZEN_COPY_TEST_OWNER_PASSWORD);
};

test("two source readers share ONE direct-owner writer from stdin: pinned TLS, identity checked, no process, pass receipt",async ()=>{
 const r=await run();
 expect(r.result.status).toBe(0);
 expect(r.result.stderr).toBe("");
 expect(r.proof).toMatchObject({processCalls:0,identityChecks:1,targetCreated:1,targetClosed:1,closedWhileWorkersActive:false,records:["org-a","org-b"],allNeoClosed:true});
 const cfg=r.proof.targetConfigs[0];
 expect(cfg).toMatchObject({host:SCHEMA_RELEASE.host,port:SCHEMA_RELEASE.port,database:"postgres",user:`postgres.${SCHEMA_RELEASE.project}`,password:"<synthetic>",application_name:"p1-frozen-copy"});
 expect(cfg.ssl.rejectUnauthorized).toBe(true);
 expect(cfg.ssl.servername).toBe(SCHEMA_RELEASE.host);
 expect(cfg.ssl.ca).toContain("BEGIN CERTIFICATE");
 expect(cfg.connectionString).toBeUndefined();
 expect(cfg.options).not.toMatch(/role|session_authorization/i);
 expect(r.receipt).toMatchObject({receipt:"frozen-source-copy-job-v3",mode:"copy",outcome:"pass",status:"COPIED",code:null,
  supervisor:{repository_id:FROZEN_CUSTODY.repositoryId,workflow:FROZEN_CUSTODY.workflow,environment:FROZEN_CUSTODY.environment,run_id:RUN_ID,run_attempt:1,
   source_sha:SOURCE_SHA,actor:FROZEN_CUSTODY.dispatcher.login,triggering_actor:FROZEN_CUSTODY.dispatcher.login,plan_run:PLAN_RUN,action:"copy",ci_conclusion:"success"},
  progress:{schemaVersion:1,stateTable:"sessions.__96bulls_frozen_copy_progress",totalOrgs:2,completedOrgs:2,pages:4,rows:2,resumeCount:0},
  budget:{concurrency:2,pageSizes:{records:5000,receipts:1000},maxPages:0,exceeded:false},
  abort:{requested:false,inFlightResolved:true,sourceClosed:true,targetClosed:true,writesStopped:true},
  unknownCommit:{detected:false,reconciliation:"NOT_REQUIRED",noBlindRetry:true}});
 expect(r.receipt.progress.bindingSha).toMatch(/^[0-9a-f]{64}$/);
 expect(r.receipt.progress.cursorSha256).toMatch(/^[0-9a-f]{64}$/);
 // Every page committed with its checkpoint: records 1 row + receipts 0 rows, both complete, per org.
 expect(r.proof.checkpoints).toEqual({
  "org-a|receipts":{page_seq:1,rows:0,status:"complete"},"org-a|records":{page_seq:1,rows:1,status:"complete"},
  "org-b|receipts":{page_seq:1,rows:0,status:"complete"},"org-b|records":{page_seq:1,rows:1,status:"complete"}});
 expect(r.proof.txLog.filter((x:string)=>x==="COMMIT").length).toBe(4);
 expect(r.proof.ownershipLocks).toBe(1);
 // Sanitized: no org id or raw key anywhere in the receipt or the log lines.
 expect(JSON.stringify(r.receipt)).not.toMatch(/org-a|org-b/);
 expect(r.result.stdout.split("\n").filter(l=>!l.includes("syntheticCopyProof")).join("\n")).not.toMatch(/org-a|org-b/);
 noSecret(r);
});

test("copy error drains both source readers before closing the one owner writer; refused receipt",async ()=>{
 const r=await run({fail:"org-a"});
 expect(r.result.status).toBe(1);
 expect(r.result.stdout).toContain("synthetic_target_failure");
 expect(r.proof).toMatchObject({targetCreated:1,targetClosed:1,closedWhileWorkersActive:false,allNeoClosed:true});
 expect(r.receipt).toMatchObject({outcome:"refused",status:"REFUSED",code:"frozen_copy_sql_XX000",abort:{inFlightResolved:true,writesStopped:true,targetClosed:true}});
 // The failed page rolled back: no org-a records and no org-a checkpoint (a page without its checkpoint is impossible).
 expect(r.proof.records).not.toContain("org-a");
 expect(r.proof.checkpoints["org-a|records"]).toBeUndefined();
 expect(r.receipt.progress.completedOrgs).toBeLessThan(2);
 noSecret(r);
});

test("an answerless failure BEFORE COMMIT is not-committed (rolled back), not unknown: fails closed, drains",async ()=>{
 const r=await run({fail:"org-a",undefinedFailure:true});
 expect(r.result.status).toBe(1);
 expect(r.proof).toMatchObject({targetCreated:1,targetClosed:1,closedWhileWorkersActive:false,allNeoClosed:true});
 expect(r.receipt).toMatchObject({outcome:"refused",status:"REFUSED",code:"frozen_copy_page_not_committed",unknownCommit:{detected:false}});
 expect(r.proof.checkpoints["org-a|records"]).toBeUndefined();
});

test("a COMMIT whose answer is lost is UNKNOWN_COMMIT, read back from the checkpoint row, never retried",async ()=>{
 const r=await run({lost:"org-a"});
 expect(r.result.status).toBe(1);
 expect(r.receipt).toMatchObject({mode:"copy",outcome:"unknown-commit",status:"UNKNOWN_COMMIT",code:"frozen_copy_unknown_commit",
  unknownCommit:{detected:true,phase:"copy",dataset:"records",checkpointSeq:1,reconciliation:"COMMITTED",noBlindRetry:true}});
 expect(r.receipt.unknownCommit.readbackAt).toBeString();
 expect(r.receipt.unknownCommit.resolvedAt).toBeString();
 expect(r.receipt.next).toContain("do not retry blindly");
 expect(JSON.stringify(r.receipt)).not.toContain("org-a");
 // It DID commit, together with its checkpoint: that is what the readback found, and no second COMMIT for it was sent.
 expect(r.proof.records.filter((x:string)=>x==="org-a")).toEqual(["org-a"]);
 expect(r.proof.checkpoints["org-a|records"]).toEqual({page_seq:1,rows:1,status:"complete"});
});

test("SIGTERM mid-copy: the in-flight page resolves, nothing new starts, receipt ABORTED, checkpoints kept",async ()=>{
 const r=await run({env:{FROZEN_COPY_TEST_SIGNAL:"org-a"}});
 expect(r.result.status).toBe(1);
 expect(r.receipt).toMatchObject({outcome:"aborted",status:"ABORTED",code:"frozen_copy_aborted",
  abort:{requested:true,observed:true,boundedReason:"SIGTERM",inFlightResolved:true,sourceClosed:true,targetClosed:true,writesStopped:true}});
 expect(r.receipt.abort.requestedAt).toBeString();
 expect(r.receipt.abort.completedAt).toBeString();
 // The in-flight page committed WITH its checkpoint; org-b's page, queued behind it, never started a transaction.
 expect(r.proof.checkpoints).toEqual({"org-a|records":{page_seq:1,rows:1,status:"complete"}});
 expect(r.proof.records).toEqual(["org-a"]);
 expect(r.receipt.next).toContain("resumes from the owner-only checkpoints");
});

for (const [name,env,code] of [
 ["missing progress table",{FROZEN_COPY_TEST_PROGRESS:"missing"},"frozen_copy_progress_table_missing"],
 ["progress table without RLS",{FROZEN_COPY_TEST_PROGRESS:"unsafe"},"frozen_copy_progress_table_unsafe"],
 ["another live operator",{FROZEN_COPY_TEST_BUSY:"1"},"frozen_copy_operator_busy"],
 ["checkpoints from another generation",{FROZEN_COPY_TEST_PRIOR:"generation"},"frozen_copy_resume_binding_mismatch"],
 ["checkpoints from another operator build",{FROZEN_COPY_TEST_PRIOR:"operator"},"frozen_copy_resume_operator_mismatch"],
] as const) test(`${name} refuses before any page is written`,async ()=>{
 const r=await run({env:{...env}});
 expect(r.result.status).toBe(1);
 expect(r.receipt).toMatchObject({outcome:"refused",status:"REFUSED",code});
 expect(r.proof.records).toEqual([]);
 expect(r.proof.checkpoints).toEqual({});
 expect(r.proof.txLog).toEqual([]);
 expect(r.proof.targetClosed).toBe(1);
});

for (const side of ["source","target"]) test(`a failed ${side} connection closes every created client`,async ()=>{
 const r=await run({connect:side});
 expect(r.result.status).toBe(1);
 expect(r.result.stdout).toContain("synthetic_connect_failure");
 expect(r.proof).toMatchObject({targetCreated:1,targetClosed:1,closedWhileWorkersActive:false,allNeoClosed:true});
 expect(r.receipt).toMatchObject({outcome:"refused",code:"frozen_copy_sql_08006"});
});

test("a session whose session_user is not the owner (temporary login + role switch) refuses before any write",async ()=>{
 const r=await run({identity:"cli"});
 expect(r.result.status).toBe(1);
 expect(r.result.stdout).toContain("frozen_copy_owner_identity");
 expect(r.proof.records).toEqual([]);
 expect(r.proof.targetClosed).toBe(1);
 expect(r.receipt).toMatchObject({outcome:"refused",code:"frozen_copy_owner_identity"});
});

test("budget exhaustion aborts deterministically before any write and writes a refused 0600 receipt file",async ()=>{
 const path=join(dir,"budget.json");
 const r=await run({extra:["--budget-ms","0","--receipt",path]});
 expect(r.result.status).toBe(1);
 expect(r.proof.records).toEqual([]);
 const file=JSON.parse(readFileSync(path,"utf8"));
 expect(file).toMatchObject({outcome:"refused",status:"REFUSED",code:"frozen_copy_budget_exhausted",budget:{maxWallMs:0,exceeded:true},progress:{totalOrgs:2,completedOrgs:0,pages:0}});
 expect(statSync(path).mode&0o777).toBe(0o600);
 expect(readFileSync(path,"utf8")).not.toContain(FROZEN_COPY_TEST_OWNER_PASSWORD);
 // An existing receipt is never overwritten.
 const again=await run({extra:["--receipt",path]});
 expect(again.result.status).toBe(1);
 expect(again.result.stdout).toContain("receipt file not written");
 expect(JSON.parse(readFileSync(path,"utf8")).code).toBe("frozen_copy_budget_exhausted");
});

const refusedBeforeConnect=async (p:ReturnType<typeof run>,code:string)=>{
 const r=await p;
 expect(r.result.status).toBe(64);
 expect(r.result.stderr).toContain(code);
 expect(r.proof.targetCreated).toBe(0);
 expect(r.proof.processCalls).toBe(0);
 noSecret(r);
};
test("the owner URL is required on stdin for --copy",()=>refusedBeforeConnect(run({doc:{neonOwnerUrl:SYNTHETIC_NEON_URL}}),"credentials_missing_key.supabaseOwnerUrl"));
test("unknown stdin keys are refused",()=>refusedBeforeConnect(run({doc:{neonOwnerUrl:SYNTHETIC_NEON_URL,supabaseOwnerUrl:syntheticOwnerUrl(),supabaseProjectRef:"x"}}),"credentials_unknown_key.supabaseProjectRef"));
for (const [name,url] of [
 ["another host",syntheticOwnerUrl({host:"db.example.invalid"})],
 ["the transaction pooler port",syntheticOwnerUrl({port:6543})],
 ["another database",syntheticOwnerUrl({database:"neondb"})],
 ["a CLI temporary login user",syntheticOwnerUrl({user:`cli_login_postgres.${SCHEMA_RELEASE.project}`})],
 ["another project's owner",syntheticOwnerUrl({user:"postgres.zyxwvutsrqponmlkjihg"})],
 ["an empty password",syntheticOwnerUrl({password:""})],
] as const) test(`owner URL with ${name} is refused before any client exists`,()=>refusedBeforeConnect(run({doc:{neonOwnerUrl:SYNTHETIC_NEON_URL,supabaseOwnerUrl:url}}),"frozen_copy_owner_url_refused"));
for (const q of ["?sslmode=disable","?sslmode=no-verify","?options=-c%20role%3Dpostgres","?sslrootcert=/tmp/x"]) test(`owner URL TLS/startup override ${q} is refused`,()=>refusedBeforeConnect(run({doc:{neonOwnerUrl:SYNTHETIC_NEON_URL,supabaseOwnerUrl:syntheticOwnerUrl({query:q})}}),"frozen_copy_owner_url_options_refused"));
for (const k of ["PGPASSWORD","PGUSER","PGOPTIONS","DATABASE_URL","SUPABASE_URL_FILE"]) test(`credential environment ${k} is refused (no env fallback)`,()=>refusedBeforeConnect(run({env:{[k]:"synthetic"}}),`credential_env_refused.${k}`));

// Supervisor (causal, through the real driver): without the protected workflow's attestation nothing connects or writes.
const refusedUnsupervised=async (p:ReturnType<typeof run>,code:string,check:string,connected=false)=>{
 const r=await p;
 expect(r.result.status).toBe(1);
 expect(r.receipt).toMatchObject({receipt:"frozen-source-copy-job-v3",outcome:"refused",status:"REFUSED",code,supervisor:null});
 expect(r.result.stdout).toContain(`"check":"${check}"`);
 expect(r.proof.records).toEqual([]);
 expect(r.proof.checkpoints).toEqual({});
 expect(r.proof.txLog).toEqual([]);
 if(!connected) expect(r.proof.targetCreated).toBe(0);
 noSecret(r);
 return r;
};
test("no supervisor: a local --copy with valid credentials refuses frozen_copy_supervisor_missing before any connection",()=>
 refusedUnsupervised(run({supervised:false}),"frozen_copy_supervisor_missing","actions"));
test("a rerun (run_attempt 2) refuses unqualified before any connection",()=>
 refusedUnsupervised(run({env:{GITHUB_RUN_ATTEMPT:"2"}}),"frozen_copy_supervisor_unqualified","run_attempt"));
test("an approval typed for another page limit refuses mismatch after the binding read, before any write",()=>
 refusedUnsupervised(run({approval:approvalFor(["--max-pages","9"])}),"frozen_copy_supervisor_mismatch","approval_maxpages",true));
test("the approval matches but GitHub does not attest the environment deployment: unqualified, no write",()=>
 refusedUnsupervised(run({env:{FROZEN_COPY_TEST_GITHUB_FORGE:"statuses"}}),"frozen_copy_supervisor_unqualified","environment_deployment",true));
test("--max-pages stops the job at the page limit with a refused receipt (resumable from checkpoints)",async ()=>{
 const r=await run({extra:["--max-pages","1"]});
 expect(r.result.status).toBe(1);
 expect(r.receipt).toMatchObject({outcome:"refused",status:"REFUSED",code:"frozen_copy_page_limit",budget:{maxPages:1}});
 expect(r.receipt.supervisor).toMatchObject({action:"copy",run_id:RUN_ID});
 expect(r.receipt.progress.pages).toBeGreaterThanOrEqual(1);
});
test("--repair-head --execute is refused before any credential is read (no supervised repair action)",async ()=>{
 const child=Bun.spawn([process.execPath,"--preload",preload,script,"--repair-head","--execute","--org","o","--workspace","w","--user","u"],{stdin:"pipe",stdout:"pipe",stderr:"pipe",timeout:10000,env:{PATH:process.env.PATH}});
 await child.stdin.end();
 const [stdout,stderr,status]=await Promise.all([new Response(child.stdout).text(),new Response(child.stderr).text(),child.exited]);
 expect(status).toBe(64);
 expect(stderr).toContain("--repair-head --execute is refused");
 expect(stdout).toContain('"targetCreated":0');
});
