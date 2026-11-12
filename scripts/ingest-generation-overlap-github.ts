/**
 * Read-only GitHub gate for .github/workflows/ingest-generation-overlap.yml. Mirrors the private-ingest
 * and schema release gates: exact main head, first attempt, Khaliq's exact dispatch identity, the
 * protected session-schema-production environment (main only), and green push CI at the SHA.
 * Errors carry fixed codes only.
 */
import { PRIVATE_INGEST_RELEASE } from "../src/session-ingest/private-ingest-release.ts";
import { SCHEMA_RELEASE as S } from "../src/hoopdb/schema-release-contract.ts";
import {
  overlapOperatorInputs,
  refuseOverlapOperator as refuse,
} from "../src/hoopdb/ingest-generation-overlap-operator.ts";
import type { PrivateIngestGenerationContract } from "../src/session-ingest/private-ingest-release.ts";
import { githubGet, requireGreenPushRuns } from "./schema-release-github.ts";

type Obj = Record<string, any>;
type Get = (path: string) => Promise<any>;
export const OVERLAP_WORKFLOW = ".github/workflows/ingest-generation-overlap.yml";

export async function overlapGithubGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
  contract?: PrivateIngestGenerationContract,
) {
  const input = overlapOperatorInputs(env, contract);
  if (env.GITHUB_REPOSITORY !== S.repository || env.GITHUB_REPOSITORY_ID !== PRIVATE_INGEST_RELEASE.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" ||
      !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    refuse("overlap_gate_context");
  const base = `/repos/${S.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha) refuse("overlap_gate_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  const pinned = (actor: Obj | undefined) =>
    actor?.type === "User" && actor?.id === S.dispatcher.id && actor?.login === S.dispatcher.login;
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== OVERLAP_WORKFLOW || run.run_attempt !== 1 || !pinned(run.actor) || !pinned(run.triggering_actor))
    refuse("overlap_gate_run");
  const environment = await get(base + "/environments/" + S.environment);
  if (environment.name !== S.environment || !Number.isSafeInteger(environment.id) ||
      !Array.isArray(environment.protection_rules) ||
      environment.deployment_branch_policy?.protected_branches !== false ||
      environment.deployment_branch_policy?.custom_branch_policies !== true)
    refuse("overlap_gate_environment");
  const branches = await get(base + `/environments/${S.environment}/deployment-branch-policies?per_page=100`);
  if (branches.total_count !== 1 || branches.branch_policies?.length !== 1 ||
      branches.branch_policies[0].name !== "main" || branches.branch_policies[0].type !== "branch")
    refuse("overlap_gate_environment_branch");
  await requireGreenPushRuns(get, base, input.sha, { runs: "overlap_gate_ci", jobs: "overlap_gate_ci_jobs" });
  const reviewerRule = environment.protection_rules.find((r: Obj) => r.type === "required_reviewers");
  if (approved && reviewerRule) {
    // With environment reviewers configured, a recorded human approval for this environment is also required.
    const reviewers = (reviewerRule.reviewers ?? [])
      .filter((r: Obj) => r.type === "User" && r.reviewer?.type === "User").map((r: Obj) => r.reviewer.id);
    const history = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID + "/approvals");
    if (!Array.isArray(history) || !history.some((a: Obj) => a.state === "approved" && a.user?.type === "User" &&
        reviewers.includes(a.user.id) && a.environments?.some((e: Obj) => e.id === environment.id && e.name === S.environment)))
      refuse("overlap_gate_human_approval");
    if (history.some((a: Obj) => a.state === "rejected" && a.environments?.some((e: Obj) => e.id === environment.id)))
      refuse("overlap_gate_human_rejected");
  }
  return input;
}

if (import.meta.main)
  overlapGithubGate(process.env, githubGet(), process.argv.includes("--approved"))
    .then(() => console.log("overlap_github_gate_ok"))
    .catch(() => {
      console.error("overlap_github_gate_refused");
      process.exitCode = 1;
    });
