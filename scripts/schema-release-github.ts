/** Read-only GitHub gate. Errors contain fixed codes only. */
import {
  releaseAnchorBranch,
  releaseInputs,
  SCHEMA_RELEASE as P,
  refuse,
} from "../src/hoopdb/schema-release-contract.ts";
type Obj = Record<string, any>;
type Get = (path: string) => Promise<any>;
/** Required push workflows on main and the job names each latest run must contain. */
const REQUIRED_PUSH_RUNS = [
  ["ci.yml", ["checks", "session-read-worker / worker"]],
  [
    "pg-proof.yml",
    ["real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"],
  ],
] as const;
/**
 * For each required workflow, the latest push run on `branch` (main for the release
 * sha, the release/schema-<slot> anchor branch for target_sha) with head_sha === sha
 * must be completed/success, and every job of that run attempt completed/success
 * with head_sha === sha, the required job names included. Refuses codes.runs or
 * codes.jobs.
 */
export async function requireGreenPushRuns(
  get: Get,
  base: string,
  sha: string,
  codes: { runs: string; jobs: string },
  branch = "main",
) {
  for (const [workflow, names] of REQUIRED_PUSH_RUNS) {
    const listing = await get(
      base +
        `/actions/workflows/${workflow}/runs?event=push&head_sha=${sha}&per_page=100`,
    );
    const runs = (listing.workflow_runs ?? [])
      .filter(
        (r: Obj) =>
          r.head_sha === sha && r.head_branch === branch && r.event === "push",
      )
      .sort((a: Obj, b: Obj) => b.id - a.id);
    const latest = runs[0];
    if (
      !latest ||
      latest.status !== "completed" ||
      latest.conclusion !== "success"
    )
      refuse(codes.runs);
    const jobs = await get(
      base +
        "/actions/runs/" +
        latest.id +
        "/attempts/" +
        latest.run_attempt +
        "/jobs?per_page=100",
    );
    if (
      jobs.total_count !== jobs.jobs?.length ||
      !jobs.jobs.length ||
      jobs.jobs.some(
        (j: Obj) =>
          j.status !== "completed" ||
          j.conclusion !== "success" ||
          j.head_sha !== sha,
      ) ||
      names.some((n) => !jobs.jobs.some((j: Obj) => j.name === n))
    )
      refuse(codes.jobs);
  }
}
export async function githubReleaseGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
) {
  const input = releaseInputs(env);
  if (
    env.GITHUB_REPOSITORY !== P.repository ||
    env.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
    env.GITHUB_REF !== "refs/heads/main" ||
    env.GITHUB_SHA !== input.sha ||
    env.GITHUB_RUN_ATTEMPT !== "1" ||
    !/^[1-9][0-9]*$/.test(env.GITHUB_RUN_ID ?? "")
  )
    refuse("release_context");
  const base = `/repos/${P.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha)
    refuse("release_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (
    run.head_sha !== input.sha ||
    run.event !== "workflow_dispatch" ||
    run.head_branch !== "main" ||
    run.path !== P.workflow ||
    run.run_attempt !== 1
  )
    refuse("release_run");
  const trustedDispatcher =
    run.actor?.type === "User" &&
    run.actor?.login === P.dispatcher.login &&
    run.actor?.id === P.dispatcher.id &&
    run.triggering_actor?.type === "User" &&
    run.triggering_actor?.login === P.dispatcher.login &&
    run.triggering_actor?.id === P.dispatcher.id;
  const environment = await get(base + "/environments/" + P.environment);
  if (!Array.isArray(environment.protection_rules))
    refuse("release_environment");
  const reviewerRule = environment.protection_rules.find(
    (r: Obj) => r.type === "required_reviewers",
  );
  const reviewers =
    reviewerRule?.reviewers
      ?.filter(
        (r: Obj) => r.type === "User" && r.reviewer?.type === "User",
      )
      .map((r: Obj) => r.reviewer.id) ?? [];
  if (
    environment.name !== P.environment ||
    !Number.isSafeInteger(environment.id) ||
    (reviewerRule ? !reviewers.length : !trustedDispatcher) ||
    environment.deployment_branch_policy?.protected_branches !== false ||
    environment.deployment_branch_policy?.custom_branch_policies !== true
  )
    refuse("release_environment");
  const branches = await get(
    base +
      "/environments/" +
      P.environment +
      "/deployment-branch-policies?per_page=100",
  );
  if (
    branches.total_count !== 1 ||
    branches.branch_policies?.length !== 1 ||
    branches.branch_policies[0].name !== "main" ||
    branches.branch_policies[0].type !== "branch"
  )
    refuse("release_environment_branch");
  await requireGreenPushRuns(get, base, input.sha, {
    runs: "release_ci",
    jobs: "release_ci_jobs",
  });
  // A held-step release is anchored to the protected release/schema-<slot>
  // branch for its target: the branch must exist, be protected and have
  // target_sha as its tip (read live, so the gate re-run under the migrator
  // lock refuses an anchor ref that moved after the plan). The runtime image is
  // published at target_sha, so its own push CI and pg-proof runs on that
  // branch must be green too, exactly as strict as the release sha's on main.
  if (input.targetSha !== input.sha) {
    const branch = releaseAnchorBranch(input.target);
    if (!branch) refuse("release_anchor_ref_missing");
    const matching = await get(base + "/git/matching-refs/heads/" + branch);
    const refs = Array.isArray(matching)
      ? matching.filter((r: Obj) => r?.ref === "refs/heads/" + branch)
      : [];
    if (refs.length !== 1) refuse("release_anchor_ref_missing");
    if (
      refs[0].object?.type !== "commit" ||
      refs[0].object?.sha !== input.targetSha
    )
      refuse("release_anchor_ref_mismatch");
    const protection = await get(base + "/branches/" + branch);
    if (protection?.name !== branch || protection?.protected !== true)
      refuse("release_anchor_ref_unprotected");
    if (protection?.commit?.sha !== input.targetSha)
      refuse("release_anchor_ref_mismatch");
    await requireGreenPushRuns(
      get,
      base,
      input.targetSha,
      { runs: "release_target_ci", jobs: "release_target_ci_jobs" },
      branch,
    );
  }
  // Apply and bootstrap both consume a successful same-sha plan run under 24h old.
  if (input.mode !== "plan") {
    const plan = await get(base + "/actions/runs/" + input.planRun);
    if (
      String(plan.id) === env.GITHUB_RUN_ID ||
      plan.path !== P.workflow ||
      plan.head_sha !== input.sha ||
      plan.head_branch !== "main" ||
      plan.event !== "workflow_dispatch" ||
      plan.run_attempt !== 1 ||
      plan.status !== "completed" ||
      plan.conclusion !== "success" ||
      !Number.isFinite(Date.parse(plan.created_at)) ||
      Date.now() - Date.parse(plan.created_at) > 24 * 60 * 60 * 1000 ||
      Date.parse(plan.created_at) > Date.now()
    )
      refuse("release_plan_run");
  }
  if (approved) {
    // Private repositories on plans without environment reviewers cannot
    // create that protection rule. In that case the first-attempt workflow
    // dispatch itself must be made by the pinned account; reruns are refused.
    if (reviewerRule) {
      const history = await get(
        base + "/actions/runs/" + env.GITHUB_RUN_ID + "/approvals",
      );
      if (
        !Array.isArray(history) ||
        !history.some(
          (a: Obj) =>
            a.state === "approved" &&
            a.user?.type === "User" &&
            reviewers.includes(a.user.id) &&
            a.environments?.some(
              (e: Obj) => e.id === environment.id && e.name === P.environment,
            ),
        )
      )
        refuse("release_human_approval");
      if (
        history.some(
          (a: Obj) =>
            a.state === "rejected" &&
            a.environments?.some((e: Obj) => e.id === environment.id),
        )
      )
        refuse("release_human_rejected");
    } else if (!trustedDispatcher) refuse("release_human_dispatch");
  }
  return input;
}
export function githubGet(token = process.env.GITHUB_TOKEN): Get {
  if (!token) refuse("release_github_token");
  return async (path) => {
    const response = await fetch("https://api.github.com" + path, {
      headers: {
        Authorization: "Bearer " + token,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      signal: AbortSignal.timeout(15000),
      redirect: "error",
    });
    if (!response.ok) refuse("release_github_api");
    return response.json();
  };
}
if (import.meta.main)
  githubReleaseGate(
    process.env,
    githubGet(),
    process.argv.includes("--approved"),
  )
    .then(() => console.log("release_github_gate_ok"))
    .catch(() => {
      console.error("release_github_gate_refused");
      process.exitCode = 1;
    });
