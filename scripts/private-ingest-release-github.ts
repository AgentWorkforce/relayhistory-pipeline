/** Read-only GitHub Actions/environment gate for the private-ingest production release. */
import { appendFileSync, readFileSync } from "node:fs";
import {
  PRIVATE_INGEST_RELEASE as P,
  privateIngestRefuse,
  privateIngestReleaseInputs,
} from "../src/session-ingest/private-ingest-release.ts";

type Obj = Record<string, any>;
type Get = (path: string) => Promise<any>;

/**
 * The tracked profile's overlapGeneration, read from the checked-out SHA (the same file the operator
 * job stages). Undefined when the profile declares no overlap, which keeps the confirmation unchanged.
 */
export function trackedOverlapGeneration(path = "config/cloudflare/private-ingest-production.json"): unknown {
  let tracked: { sessionIngest?: Record<string, unknown> };
  try { tracked = JSON.parse(readFileSync(path, "utf8")); }
  catch { return privateIngestRefuse("private_ingest_release_profile"); }
  return tracked?.sessionIngest?.overlapGeneration;
}

export async function privateIngestGithubGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
  profileOverlap: unknown = trackedOverlapGeneration(),
) {
  const input = privateIngestReleaseInputs(env, profileOverlap);
  if (env.GITHUB_REPOSITORY !== P.repository || env.GITHUB_REPOSITORY_ID !== P.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" ||
      !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    privateIngestRefuse("private_ingest_release_context");
  const base = `/repos/${P.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha)
    privateIngestRefuse("private_ingest_release_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== P.workflow || run.run_attempt !== 1 ||
      run.actor?.id !== P.releaseActorGithubUserId || run.actor?.type !== "User" ||
      run.triggering_actor?.id !== P.releaseActorGithubUserId || run.triggering_actor?.type !== "User")
    privateIngestRefuse("private_ingest_release_run");
  const environment = await get(base + "/environments/" + P.environment);
  const protectionRules = environment.protection_rules ?? [];
  if (environment.name !== P.environment || !Number.isSafeInteger(environment.id) ||
      !Array.isArray(protectionRules) || protectionRules.length !== 1 ||
      protectionRules[0]?.type !== "branch_policy" ||
      environment.deployment_branch_policy?.protected_branches !== false ||
      environment.deployment_branch_policy?.custom_branch_policies !== true)
    privateIngestRefuse("private_ingest_release_environment");
  const branches = await get(base + `/environments/${P.environment}/deployment-branch-policies?per_page=100`);
  if (branches.total_count !== 1 || branches.branch_policies?.length !== 1 ||
      branches.branch_policies[0].name !== "main" || branches.branch_policies[0].type !== "branch")
    privateIngestRefuse("private_ingest_release_environment_branch");
  const listing = await get(base + `/actions/workflows/ci.yml/runs?event=push&head_sha=${input.sha}&per_page=100`);
  const runs = (listing.workflow_runs ?? [])
    .filter((row: Obj) => row.head_sha === input.sha && row.head_branch === "main" && row.event === "push")
    .sort((a: Obj, b: Obj) => b.id - a.id);
  const ci = runs[0];
  if (!ci || ci.status !== "completed" || ci.conclusion !== "success")
    privateIngestRefuse("private_ingest_release_ci");
  const jobs = await get(base + `/actions/runs/${ci.id}/attempts/${ci.run_attempt}/jobs?per_page=100`);
  if (jobs.total_count !== jobs.jobs?.length || !jobs.jobs.length ||
      jobs.jobs.some((job: Obj) => job.status !== "completed" || job.conclusion !== "success" || job.head_sha !== input.sha) ||
      !jobs.jobs.some((job: Obj) => job.name === "unit-tests") ||
      !jobs.jobs.some((job: Obj) => job.name === "session-read-worker / worker"))
    privateIngestRefuse("private_ingest_release_ci_jobs");
  if (input.mode === "deploy") {
    const plan = await get(base + "/actions/runs/" + input.planRun);
    if (String(plan.id) === env.GITHUB_RUN_ID || plan.path !== P.workflow || plan.head_sha !== input.sha ||
        plan.head_branch !== "main" || plan.event !== "workflow_dispatch" || plan.run_attempt !== 1 ||
        plan.status !== "completed" || plan.conclusion !== "success" ||
        !Number.isFinite(Date.parse(plan.created_at)) || Date.parse(plan.created_at) > Date.now() ||
        Date.now() - Date.parse(plan.created_at) > 24 * 60 * 60 * 1000)
      privateIngestRefuse("private_ingest_release_plan_run");
    const artifacts = await get(base + `/actions/runs/${input.planRun}/artifacts?per_page=100`);
    const planArtifacts = (artifacts.artifacts ?? []).filter((artifact: Obj) =>
      artifact.name === "private-ingest-plan" && artifact.expired === false);
    if (artifacts.total_count !== artifacts.artifacts?.length || planArtifacts.length !== 1)
      privateIngestRefuse("private_ingest_release_plan_artifact");
  }
  if (!Number.isFinite(Date.parse(run.created_at)) || Date.parse(run.created_at) > Date.now())
    privateIngestRefuse("private_ingest_release_authorization_date");
  return { ...input, authorizationDate: new Date(run.created_at).toISOString().slice(0, 10), approved };
}

export function privateIngestGithubGet(token = process.env.GITHUB_TOKEN): Get {
  if (!token) privateIngestRefuse("private_ingest_release_github_token");
  return async (path) => {
    const response = await fetch("https://api.github.com" + path, {
      headers: {
        Authorization: "Bearer " + token,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      signal: AbortSignal.timeout(15_000),
      redirect: "error",
    });
    if (!response.ok) privateIngestRefuse("private_ingest_release_github_api");
    return response.json();
  };
}

if (import.meta.main)
  privateIngestGithubGate(process.env, privateIngestGithubGet(), process.argv.includes("--approved"))
    .then((result) => {
      if (process.argv.includes("--approved") && process.env.GITHUB_OUTPUT)
        appendFileSync(process.env.GITHUB_OUTPUT, `authorization_date=${result.authorizationDate}\n`);
      console.log("private_ingest_release_github_gate_ok");
    })
    .catch(() => {
      console.error("private_ingest_release_github_gate_refused");
      process.exitCode = 1;
    });
