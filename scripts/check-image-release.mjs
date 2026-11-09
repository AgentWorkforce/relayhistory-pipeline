import { execFileSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

export const REPOSITORY = "AgentWorkforce/relayhistory-pipeline";
// GitHub repository id; unchanged by the 2026-11-07 rename from sessions-pipeline.
export const REPOSITORY_ID = 1437714333;
const SHA = /^[0-9a-f]{40}$/;
const refuse = (reason) => { throw new Error(`Image release refused: ${reason}`); };

export function validateRequest(env) {
  if (env.GITHUB_REPOSITORY !== REPOSITORY ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
      env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_ACTOR !== "khaliqgant" || env.GITHUB_TRIGGERING_ACTOR !== "khaliqgant" ||
      env.GITHUB_WORKFLOW_REF !== `${REPOSITORY}/.github/workflows/publish-image.yml@refs/heads/main` ||
      env.CONFIRM !== "publish 96bulls-services") refuse("dispatcher or workflow identity");
  if (typeof env.SOURCE_SHA !== "string" || !SHA.test(env.SOURCE_SHA) ||
      typeof env.GITHUB_SHA !== "string" || !SHA.test(env.GITHUB_SHA)) refuse("full lowercase source/workflow SHA required");
  const note = env.APPROVAL_NOTE;
  if (typeof note !== "string" || !note.trim() || Buffer.byteLength(note) > 240 ||
      /[\u0000-\u001f\u007f\u2028\u2029]/u.test(note)) refuse("single-line non-sensitive approval reference required");
  return { sourceSha: env.SOURCE_SHA, workflowSha: env.GITHUB_SHA, note: note.trim() };
}

export function verifyAncestry(sourceSha, run = execFileSync, token = process.env.GH_TOKEN) {
  if (!SHA.test(sourceSha)) refuse("full source SHA required");
  // Checkout intentionally persists no credentials. Authenticate this one fetch
  // through process configuration, never argv, a credential file or git config.
  const fetchOptions = { stdio: "pipe", ...(token ? { env: {
    ...process.env, GIT_CONFIG_COUNT: "1",
    GIT_CONFIG_KEY_0: "http.https://github.com/.extraheader",
    GIT_CONFIG_VALUE_0: `AUTHORIZATION: basic ${Buffer.from(`x-access-token:${token}`).toString("base64")}`,
  } } : {}) };
  try { run("git", ["fetch", "--no-tags", "origin", "main"], fetchOptions); }
  catch { refuse("cannot fetch current main"); }
  try {
    run("git", ["cat-file", "-e", `${sourceSha}^{commit}`], { stdio: "pipe" });
    run("git", ["merge-base", "--is-ancestor", sourceSha, "origin/main"], { stdio: "pipe" });
  } catch { refuse("source is not a commit on current main"); }
}

async function effectiveJobs(base, run, sourceSha, getJSON) {
  // Failed-jobs-only reruns carry successful jobs from earlier attempts. Read
  // every execution and retain each named job's newest attempt; an old success
  // must never hide a failed retry. CI job names must be unambiguous.
  const seenIds = new Set();
  const executions = new Set();
  const latestByName = new Map();
  let total;
  for (let page = 1; ; page++) {
    const data = await getJSON(`${base}/runs/${run.id}/jobs?filter=all&per_page=100&page=${page}`);
    if (!Number.isSafeInteger(data.total_count) || data.total_count < 1 || data.total_count > 10_000 ||
        (total !== undefined && data.total_count !== total) || !Array.isArray(data.jobs) ||
        !data.jobs.length || data.jobs.length > 100) refuse("source CI job metadata is incomplete or changing");
    total = data.total_count;
    for (const job of data.jobs) {
      if (!Number.isSafeInteger(job.id) || job.id < 1 || seenIds.has(job.id) || job.run_id !== run.id ||
          job.head_sha !== sourceSha || typeof job.name !== "string" || !job.name.trim() ||
          !Number.isSafeInteger(job.run_attempt) || job.run_attempt < 1 || job.run_attempt > run.run_attempt) {
        refuse("invalid source CI job identity");
      }
      const key = JSON.stringify([job.name, job.run_attempt]);
      if (executions.has(key)) refuse("ambiguous source CI job names");
      seenIds.add(job.id);
      executions.add(key);
      const previous = latestByName.get(job.name);
      if (!previous || job.run_attempt > previous.run_attempt) latestByName.set(job.name, job);
    }
    if (seenIds.size > total) refuse("source CI job metadata is incomplete or changing");
    if (seenIds.size === total) return [...latestByName.values()];
  }
}

export async function verifySourceCI(sourceSha, getJSON) {
  if (!SHA.test(sourceSha)) refuse("full source SHA required");
  const base = `/repos/${REPOSITORY}/actions`;
  const runsPath = `${base}/workflows/ci.yml/runs?head_sha=${sourceSha}&event=push&branch=main&per_page=100`;
  const response = await getJSON(runsPath);
  if (!Array.isArray(response.workflow_runs)) refuse("missing source CI metadata");
  // Never fall back to an older green run when the newest matching run is red or pending.
  const matching = response.workflow_runs.filter(run => run.head_sha === sourceSha &&
    run.head_branch === "main" && run.event === "push" &&
    run.repository?.full_name === REPOSITORY && run.head_repository?.full_name === REPOSITORY);
  if (!matching.length) refuse("source has no official main CI run");
  if (matching.some(run => !Number.isSafeInteger(run.id) || run.id <= 0)) refuse("invalid CI identity");
  const latest = matching.reduce((a, b) => a.id > b.id ? a : b);
  if (latest.status !== "completed" || latest.conclusion !== "success" ||
      !Number.isSafeInteger(latest.run_attempt) || latest.run_attempt < 1) refuse("latest source CI is not successful");
  const jobs = await effectiveJobs(base, latest, sourceSha, getJSON);
  if (!jobs.every(job => job.status === "completed" && job.conclusion === "success") ||
      !jobs.some(job => job.name === "checks")) refuse("source CI jobs are incomplete or unsuccessful");
  const refreshed = await getJSON(runsPath);
  if (!Array.isArray(refreshed.workflow_runs)) refuse("source CI changed during validation");
  const refreshedMatching = refreshed.workflow_runs.filter(run => run.head_sha === sourceSha &&
    run.head_branch === "main" && run.event === "push" && run.repository?.full_name === REPOSITORY &&
    run.head_repository?.full_name === REPOSITORY);
  if (!refreshedMatching.length || refreshedMatching.some(run => !Number.isSafeInteger(run.id) || run.id <= 0)) refuse("source CI changed during validation");
  const newest = refreshedMatching.reduce((a, b) => a.id > b.id ? a : b);
  if (newest.id !== latest.id || newest.run_attempt !== latest.run_attempt ||
      newest.status !== "completed" || newest.conclusion !== "success") refuse("source CI changed during validation");
  const current = await getJSON(`${base}/runs/${latest.id}`);
  if (current.head_sha !== sourceSha || current.run_attempt !== latest.run_attempt ||
      current.status !== "completed" || current.conclusion !== "success") refuse("source CI changed during validation");
  return latest.id;
}

export function githubJSON(token, fetcher = fetch) {
  if (!token) refuse("GitHub metadata token required");
  return async path => {
    const response = await fetcher(`https://api.github.com${path}`, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" },
      redirect: "error", signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) refuse(`GitHub metadata HTTP ${response.status}`);
    try { return await response.json(); } catch { refuse("invalid GitHub metadata"); }
  };
}

export function assertExpectedCI(ciRun, expected) {
  if (expected !== undefined && (!/^[1-9][0-9]*$/.test(expected) || String(ciRun) !== expected))
    refuse("source CI run changed after signing");
}

async function main() {
  const request = validateRequest(process.env);
  verifyAncestry(request.sourceSha);
  const ciRun = await verifySourceCI(request.sourceSha, githubJSON(process.env.GH_TOKEN));
  assertExpectedCI(ciRun, process.env.EXPECTED_CI_RUN);
  if (!process.env.GITHUB_OUTPUT) refuse("workflow output missing");
  appendFileSync(process.env.GITHUB_OUTPUT, `sha=${request.sourceSha}\nci_run_id=${ciRun}\napproval_note=${request.note}\n`);
  console.log(`Approved source ${request.sourceSha}; successful main CI run ${ciRun}.`);
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
