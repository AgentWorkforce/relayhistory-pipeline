import {test,expect} from "bun:test";
import {spawnSync} from "node:child_process";
import {fileURLToPath} from "node:url";
const script=fileURLToPath(new URL("../../../scripts/frozen-source-copy.ts",import.meta.url));
const preload=fileURLToPath(new URL("./fixtures/frozen-copy-preload.ts",import.meta.url));
// Synthetic Supabase ref; it matches the preload's fake CLI login host.
const REF="abcdefghijklmnopqrst";
function run(fail?:string, undefinedFailure=false, connectFailure?:string, ref:string|null=REF) {
 const result=spawnSync(process.execPath,["--preload",preload,script,"--copy","--concurrency","2"],{
  input:JSON.stringify({neonOwnerUrl:"postgresql://neondb_owner:synthetic-not-authorized@ep-floral-pond-apkvat38.synthetic.invalid/neondb"})+"\n",
  encoding:"utf8",timeout:10000,env:{PATH:process.env.PATH,SUPABASE_CLI:"/synthetic-supabase-refuse-network",...(ref===null?{}:{SUPABASE_PROJECT_REF:ref}),...(fail?{FROZEN_COPY_TEST_FAIL:fail}:{}),...(undefinedFailure?{FROZEN_COPY_TEST_UNDEFINED:"1"}:{}),...(connectFailure?{FROZEN_COPY_TEST_CONNECT:connectFailure}:{})},
 });
 const proof=result.stdout.split("\n").filter(x=>x.startsWith("{")).map(x=>JSON.parse(x)).find(x=>x.syntheticCopyProof)?.syntheticCopyProof;
 return {result,proof};
}
test("two source readers reuse one authenticated owner writer without rotating CLI passwords",()=>{
 const {result,proof}=run();
 expect(result.status).toBe(0);
 expect(proof).toEqual({logins:1,targetCreated:1,targetClosed:1,closedWhileWorkersActive:false,records:["org-a","org-b"],allNeoClosed:true});
 expect(result.stdout).not.toContain("synthetic-cli-password");
 expect(result.stderr).toBe("");
});
test("copy error drains both source readers before closing the one owner writer",()=>{
 const {result,proof}=run("org-a");
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("synthetic_target_failure");
 expect(proof.logins).toBe(1);
 expect(proof.targetCreated).toBe(1);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
 expect(result.stdout).not.toContain("synthetic-cli-password");
});

test("an undefined worker rejection still fails closed and drains the shared writer",()=>{
 const {result,proof}=run("org-a",true);
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("copy_worker_failed");
 expect(proof.targetCreated).toBe(1);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
});

for (const side of ["source","target"]) test(`a failed ${side} connection closes every created client`,()=>{
 const {result,proof}=run(undefined,false,side);
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("synthetic_connect_failure");
 expect(proof.logins).toBe(1);
 expect(proof.targetCreated).toBe(1);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
});

test("the Supabase project ref is required configuration: unset or mismatched refuses before any login is used",()=>{
 const unset=run(undefined,false,undefined,null);
 expect(unset.result.status).toBe(1);
 expect(unset.result.stdout).toContain("supabase_project_ref_required");
 expect(unset.proof.logins).toBe(0);
 expect(unset.proof.records).toEqual([]);
 const other=run(undefined,false,undefined,"zyxwvutsrqponmlkjihg");
 expect(other.result.status).toBe(1);
 expect(other.result.stdout).toContain("cli_target_pin");
 expect(other.proof.records).toEqual([]);
 expect(other.result.stdout).not.toContain("synthetic-cli-password");
});
