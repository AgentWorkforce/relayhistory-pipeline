import { expect, test } from "bun:test";
import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { assertExpectedCI, githubJSON, REPOSITORY, validateRequest, verifyAncestry, verifySourceCI } from "../../../scripts/check-image-release.mjs";
const sha = "a".repeat(40);
const env = () => ({ GITHUB_REPOSITORY: REPOSITORY, GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_REF: "refs/heads/main", GITHUB_ACTOR: "khaliqgant", GITHUB_TRIGGERING_ACTOR: "khaliqgant", GITHUB_WORKFLOW_REF: `${REPOSITORY}/.github/workflows/publish-image.yml@refs/heads/main`, GITHUB_SHA: "b".repeat(40), SOURCE_SHA: sha, CONFIRM: "publish 96bulls-services", APPROVAL_NOTE: "Approved in operator chat" });
const run = () => ({ id: 123, run_attempt: 1, head_sha: sha, head_branch: "main", event: "push", repository: { full_name: REPOSITORY }, head_repository: { full_name: REPOSITORY }, status: "completed", conclusion: "success" });
const jobs = () => ({ total_count: 1, jobs: [{ id: 1, run_id: 123, run_attempt: 1, name: "checks", head_sha: sha, status: "completed", conclusion: "success" }] });
const api = (runs = [run()], jobData = jobs()) => async (path: string) => path.includes("/workflows/") ? { workflow_runs: runs } : path.includes("/jobs?") ? jobData : runs.reduce((a, b) => a.id > b.id ? a : b);
test("accepts an explicit owner/main/source release with a public approval reference", () => expect(validateRequest(env())).toEqual({ sourceSha: sha, workflowSha: "b".repeat(40), note: "Approved in operator chat" }));
test("refuses publication from the duplicate import even under the same owner", () => {
 const duplicate = "AgentWorkforce/sessions-pipeline-oss";
 expect(() => validateRequest({ ...env(), GITHUB_REPOSITORY: duplicate, GITHUB_WORKFLOW_REF: `${duplicate}/.github/workflows/publish-image.yml@refs/heads/main` })).toThrow("dispatcher or workflow identity");
});
for (const [field, value] of Object.entries({ GITHUB_REPOSITORY: "fork/sessions-pipeline", GITHUB_EVENT_NAME: "push", GITHUB_REF: "refs/heads/topic", GITHUB_ACTOR: "another-actor", GITHUB_TRIGGERING_ACTOR: "another-actor", GITHUB_WORKFLOW_REF: `${REPOSITORY}/.github/workflows/other.yml@refs/heads/main`, CONFIRM: "publish", GITHUB_SHA: "bad" })) {
  test(`refuses foreign ${field}`, () => expect(() => validateRequest({ ...env(), [field]: value })).toThrow());
}
for (const value of ["", "main", "a".repeat(7), "A".repeat(40), sha + "\n", "a".repeat(39) + ";"]) test(`refuses invalid source ${JSON.stringify(value)}`, () => expect(() => validateRequest({ ...env(), SOURCE_SHA: value })).toThrow());
for (const note of ["", "  ", "line\norg.opencontainers.image.revision=other", "line\rnext", "line\u2028next", "x".repeat(241)]) test(`refuses unsafe note ${JSON.stringify(note)}`, () => expect(() => validateRequest({ ...env(), APPROVAL_NOTE: note })).toThrow());
for (const failingCommand of ["fetch", "cat-file", "merge-base"]) {
 test(`sanitizes ${failingCommand} failures and exercises the ancestry gate`, () => {
  let message = ""; const calls: string[] = [];
  try { verifyAncestry(sha, (_command: string, args: string[]) => { calls.push(args[0]); if(args[0] === failingCommand) throw new Error("private diagnostic"); }); }
  catch (error) { message = (error as Error).message; }
  expect(calls).toEqual(["fetch", "cat-file", "merge-base"].slice(0,["fetch", "cat-file", "merge-base"].indexOf(failingCommand)+1));
  expect(message).toBe(`Image release refused: ${failingCommand === "fetch" ? "cannot fetch current main" : "source is not a commit on current main"}`);
  expect(message).not.toContain("private diagnostic");
 });
}
test("accepts only the latest exact-source successful main run and its jobs", async () => expect(await verifySourceCI(sha, api())).toBe(123));
test("private ancestry fetch uses only ephemeral process authentication", () => {
 const calls: {args:string[];options:any}[]=[];
 verifyAncestry(sha, (_command:string,args:string[],options:any)=>{calls.push({args,options});}, "fixture-token-only");
 expect(calls.map(c=>c.args[0])).toEqual(["fetch","cat-file","merge-base"]);
 expect(calls[0].options.env.GIT_CONFIG_VALUE_0).toBe(`AUTHORIZATION: basic ${Buffer.from("x-access-token:fixture-token-only").toString("base64")}`);
 expect(calls.every(c=>!c.args.join(" ").includes("fixture-token-only"))).toBe(true);
 expect(calls.slice(1).every(c=>c.options.env===undefined)).toBe(true);
});
test("a new CI run after signing cannot authorize copying the older signed release", () => {
 expect(()=>assertExpectedCI(123,undefined)).not.toThrow();
 expect(()=>assertExpectedCI(123,"123")).not.toThrow();
 for(const expected of ["124","","0"," 123"]) expect(()=>assertExpectedCI(123,expected)).toThrow("changed after signing");
});
test("refuses green CI from the duplicate import", async () => {
 const duplicate = { full_name: "AgentWorkforce/sessions-pipeline-oss" };
 await expect(verifySourceCI(sha, api([{ ...run(), repository: duplicate, head_repository: duplicate }]))).rejects.toThrow("no official main CI run");
});
for (const patch of [{ head_sha: "c".repeat(40) }, { head_branch: "topic" }, { event: "pull_request" }, { repository: { full_name: "fork/repo" } }, { head_repository: { full_name: "fork/repo" } }, { status: "in_progress" }, { conclusion: "failure" }, { conclusion: "cancelled" }, { conclusion: "skipped" }, { run_attempt: 0 }, { id: -1 }]) {
  test(`refuses invalid CI ${JSON.stringify(patch)}`, async () => { await expect(verifySourceCI(sha, api([{ ...run(), ...patch }]))).rejects.toThrow(); });
}
test("does not reuse an older green run after a newer failure", async () => { await expect(verifySourceCI(sha, api([run(), { ...run(), id: 124, conclusion: "failure" }]))).rejects.toThrow(); });
for (const data of [{}, { workflow_runs: [] }]) test(`refuses missing CI ${JSON.stringify(data)}`, async () => { await expect(verifySourceCI(sha, async () => data)).rejects.toThrow(); });
for (const patch of [{ name: "other" }, { head_sha: "c".repeat(40) }, { status: "in_progress" }, { conclusion: "skipped" }, { conclusion: "failure" }]) {
  test(`refuses unverified job ${JSON.stringify(patch)}`, async () => { await expect(verifySourceCI(sha, api([run()], { total_count: 1, jobs: [{ ...jobs().jobs[0], ...patch }] }))).rejects.toThrow(); });
}
test("refuses incomplete job pagination", async () => { await expect(verifySourceCI(sha, api([run()], { ...jobs(), total_count: 2 }))).rejects.toThrow(); });
test("accepts a successful worker-only rerun with the required gate carried from attempt one", async () => {
 const latest = { ...run(), run_attempt: 2 };
 const worker = { ...jobs().jobs[0], id: 2, name: "session-read-worker / worker", conclusion: "failure" };
 const data = { total_count: 3, jobs: [jobs().jobs[0], worker, { ...worker, id: 3, run_attempt: 2, conclusion: "success" }] };
 const paths: string[] = [];
 const get = async (path: string) => { paths.push(path); return api([latest], data)(path); };
 expect(await verifySourceCI(sha, get)).toBe(123);
 expect(paths).toContain(`/repos/${REPOSITORY}/actions/runs/123/jobs?filter=all&per_page=100&page=1`);
 expect(paths.some(path => path.includes("/attempts/"))).toBe(false);
});
for (const conclusion of ["failure", "cancelled", "skipped"]) test(`an old successful gate cannot hide its ${conclusion} retry`, async () => {
 const latest = { ...run(), run_attempt: 2 };
 const data = { total_count: 2, jobs: [{ ...jobs().jobs[0], id: 2, run_attempt: 2, conclusion }, jobs().jobs[0]] };
 await expect(verifySourceCI(sha, api([latest], data))).rejects.toThrow("incomplete or unsuccessful");
});
for (const patch of [{ id: 0 }, { run_id: 124 }, { run_attempt: 0 }, { run_attempt: 2 }, { name: " " }]) test(`refuses invalid job identity ${JSON.stringify(patch)}`, async () => {
 await expect(verifySourceCI(sha, api([run()], { total_count: 1, jobs: [{ ...jobs().jobs[0], ...patch }] }))).rejects.toThrow("job identity");
});
test("refuses ambiguous same-name jobs within one attempt", async () => {
 await expect(verifySourceCI(sha, api([run()], { total_count: 2, jobs: [jobs().jobs[0], { ...jobs().jobs[0], id: 2 }] }))).rejects.toThrow("ambiguous");
});
for (const failingPage of [false, true]) test(`validates a required gate on page two, failingPage=${failingPage}`, async () => {
 const first = Array.from({ length: 100 }, (_, i) => ({ ...jobs().jobs[0], id: i + 2, name: `job-${i}` }));
 const paths: string[] = [];
 const get = async (path: string) => {
  paths.push(path);
  if (path.includes("/jobs?")) return { total_count: 101, jobs: path.endsWith("page=1") ? first : [{ ...jobs().jobs[0], conclusion: failingPage ? "failure" : "success" }] };
  return api()(path);
 };
 if (failingPage) await expect(verifySourceCI(sha, get)).rejects.toThrow("incomplete or unsuccessful");
 else expect(await verifySourceCI(sha, get)).toBe(123);
 expect(paths.filter(path => path.includes("/jobs?"))).toHaveLength(2);
});
for (const secondPage of [{ total_count: 3, jobs: [{ ...jobs().jobs[0], id: 2, name: "other" }] }, { total_count: 2, jobs: [] }]) test(`refuses incomplete/changing later job page ${JSON.stringify(secondPage)}`, async () => {
 let pages = 0;
 const get = async (path: string) => path.includes("/jobs?") ? ++pages === 1 ? { ...jobs(), total_count: 2 } : secondPage : api()(path);
 await expect(verifySourceCI(sha, get)).rejects.toThrow("incomplete or changing");
});
test("GitHub metadata errors fail closed without reflecting response bodies", async () => { const get = githubJSON("test-only", async () => new Response("private diagnostic", { status: 403 })); let message = ""; try { await get("/repos/test"); } catch(error) { message = (error as Error).message; }
 expect(message).toBe("Image release refused: GitHub metadata HTTP 403"); expect(message).not.toContain("private diagnostic"); });
test("rejects malformed API JSON", async () => { const get = githubJSON("test-only", async () => new Response("not json")); await expect(get("/repos/test")).rejects.toThrow("invalid GitHub metadata"); });

test("refuses a rerun that starts during validation", async () => {
 const get = async (path: string) => path.includes("/workflows/") ? {workflow_runs:[run()]} : path.includes("/jobs?") ? jobs() : {...run(),run_attempt:2,status:"in_progress"};
 await expect(verifySourceCI(sha,get)).rejects.toThrow("changed during validation");
});

for (const status of ["in_progress", "completed"]) test(`refuses a newer CI run appearing mid-validation: ${status}`, async () => {
 let lists=0; const newer={...run(),id:124,status,conclusion:status === "completed" ? "failure" : null};
 const get=async(path:string)=>path.includes("/workflows/") ? {workflow_runs:++lists===1 ? [run()] : [newer,run()]} : path.includes("/jobs?") ? jobs() : run();
 await expect(verifySourceCI(sha,get)).rejects.toThrow("changed during validation");
});

test("documented verification stops if build provenance fails", () => {
 const doc=readFileSync(new URL("../../../docs/services-image-release.md",import.meta.url),"utf8");
 const block=doc.match(/```bash\n([\s\S]*?)```/)?.[1]; expect(block).toBeDefined();
 const dir=mkdtempSync(join(tmpdir(),"image-verification-"));
 try {
  const script=`node() { return 42; }; cosign() { echo unexpected-later-check; };\n${block}\necho unexpected-later-check`;
  const result=Bun.spawnSync(["bash","-c",script],{cwd:dir,env:{PATH:process.env.PATH??"",image:"ghcr.io/agentworkforce/96bulls-services@sha256:"+"c".repeat(64),source_sha:sha,workflow_sha:sha,ci_run_id:"123",publish_run_id:"456",publish_run_attempt:"1",receipt_dir:dir},stdout:"pipe",stderr:"pipe"});
  expect(result.exitCode).toBe(42);expect(result.stdout.toString()).not.toContain("unexpected-later-check");
 } finally {rmSync(dir,{recursive:true,force:true});}
});
