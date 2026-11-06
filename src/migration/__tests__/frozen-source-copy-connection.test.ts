import {test,expect} from "bun:test";
import {spawnSync} from "node:child_process";
import {fileURLToPath} from "node:url";
const script=fileURLToPath(new URL("../../../scripts/frozen-source-copy.ts",import.meta.url));
const preload=fileURLToPath(new URL("./fixtures/frozen-copy-preload.ts",import.meta.url));
function run(fail?:string, undefinedFailure=false, connectFailure?:string, badRowCount?:string) {
 const result=spawnSync(process.execPath,["--preload",preload,script,"--copy","--concurrency","2"],{
  input:JSON.stringify({neonOwnerUrl:"postgresql://neondb_owner:synthetic-not-authorized@ep-floral-pond-apkvat38.synthetic.invalid/neondb"})+"\n",
  encoding:"utf8",timeout:10000,env:{PATH:process.env.PATH,SUPABASE_CLI:"/synthetic-supabase-refuse-network",...(fail?{FROZEN_COPY_TEST_FAIL:fail}:{}),...(undefinedFailure?{FROZEN_COPY_TEST_UNDEFINED:"1"}:{}),...(connectFailure?{FROZEN_COPY_TEST_CONNECT:connectFailure}:{}),...(badRowCount?{FROZEN_COPY_TEST_BAD_ROWCOUNT:badRowCount}:{})},
 });
 const logs=result.stdout.split("\n").filter(x=>x.startsWith("{")).map(x=>JSON.parse(x));
 const proof=logs.find(x=>x.syntheticCopyProof)?.syntheticCopyProof;
 return {result,proof,logs,periodic:logs.filter(x=>x.m==="copy_metrics"),final:logs.filter(x=>x.m==="copy_metrics_final")};
}
test("two source readers reuse one authenticated owner writer without rotating CLI passwords",()=>{
 const {result,proof,periodic,final}=run();
 expect(result.status).toBe(0);
 expect(proof).toEqual({logins:1,targetCreated:1,targetClosed:1,closedWhileWorkersActive:false,recordAttempts:2,recordInserted:1,receiptAttempts:2,receiptInserted:1,deletions:1,resets:1,allNeoClosed:true});
 expect(periodic.length).toBeGreaterThan(0);
 expect(final).toHaveLength(1);
 expect(final[0]).toMatchObject({orgsStarted:2,orgsCompleted:2,recordPages:2,recordRowsRead:2,recordRowsAttempted:2,recordRowsInserted:1,recordRowsConflicted:1,receiptPages:2,receiptRowsRead:2,receiptRowsAttempted:2,receiptRowsInserted:1,receiptRowsConflicted:1,targetPending:0});
 for(const k of ["elapsedMs","setupMs","recordSourceQueryMs","recordSourceQueryMaxMs","recordTargetClientMs","recordTargetClientMaxMs","receiptSourceQueryMs","receiptTargetClientMs","targetPendingMax","backoffMs"])expect(final[0][k]).toBeGreaterThan(0);
 expect(final[0].backoffEvents).toBeGreaterThan(0);
 expect(final[0].recordRowsInserted+final[0].recordRowsConflicted).toBe(final[0].recordRowsAttempted);
 expect(final[0].receiptRowsInserted+final[0].receiptRowsConflicted).toBe(final[0].receiptRowsAttempted);
 const fields=["at","m","elapsedMs","setupMs","orgsStarted","orgsCompleted","recordPages","recordRowsRead","recordRowsAttempted","recordRowsInserted","recordRowsConflicted","recordSourceQueryMs","recordSourceQueryMaxMs","recordTargetClientMs","recordTargetClientMaxMs","receiptPages","receiptRowsRead","receiptRowsAttempted","receiptRowsInserted","receiptRowsConflicted","receiptSourceQueryMs","receiptTargetClientMs","targetPending","targetPendingMax","backoffEvents","backoffMs"].sort();
 for(const snapshot of [...periodic,...final]){
  expect(Object.keys(snapshot).sort()).toEqual(fields);
  expect(typeof snapshot.at).toBe("string");expect(typeof snapshot.m).toBe("string");
  for(const [key,value] of Object.entries(snapshot))if(key!=="at"&&key!=="m")expect(Number.isSafeInteger(value)).toBe(true);
 }
 for(let i=1;i<periodic.length;i++)expect(periodic[i].elapsedMs-periodic[i-1].elapsedMs).toBeGreaterThanOrEqual(15000);
 for(const secret of ["org-a","org-b","private-org","private-workspace","private-user","private-origin","private-source","private-session","reset-org","reset-workspace","reset-user","reset-request","synthetic-cli-password"])expect(result.stdout).not.toContain(secret);
 expect(result.stderr).toBe("");
});

test("an impossible target rowCount fails closed without inventing insert or conflict counts",()=>{
 const {result,proof,final}=run(undefined,false,undefined,"org-a");
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("copy target returned an invalid inserted-row count");
 expect(final).toHaveLength(1);
 expect(final[0].recordRowsAttempted).toBeLessThanOrEqual(proof.recordAttempts);
 expect(final[0].recordRowsInserted+final[0].recordRowsConflicted).toBe(final[0].recordRowsAttempted);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
});
test("copy error drains both source readers before closing the one owner writer",()=>{
 const {result,proof,final}=run("org-a");
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("synthetic_target_failure");
 expect(final).toHaveLength(1);
 expect(final[0].targetPending).toBe(0);
 expect(proof.logins).toBe(1);
 expect(proof.targetCreated).toBe(1);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
 expect(result.stdout).not.toContain("synthetic-cli-password");
});

test("an undefined worker rejection still fails closed and drains the shared writer",()=>{
 const {result,proof,final}=run("org-a",true);
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("copy_worker_failed");
 expect(final).toHaveLength(1);
 expect(final[0].targetPending).toBe(0);
 expect(proof.targetCreated).toBe(1);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
});

for (const side of ["source","target"]) test(`a failed ${side} connection closes every created client`,()=>{
 const {result,proof,final}=run(undefined,false,side);
 expect(result.status).toBe(1);
 expect(result.stdout).toContain("synthetic_connect_failure");
 expect(final).toHaveLength(1);
 expect(final[0].targetPending).toBe(0);
 expect(proof.logins).toBe(1);
 expect(proof.targetCreated).toBe(1);
 expect(proof.targetClosed).toBe(1);
 expect(proof.closedWhileWorkersActive).toBe(false);
 expect(proof.allNeoClosed).toBe(true);
});
