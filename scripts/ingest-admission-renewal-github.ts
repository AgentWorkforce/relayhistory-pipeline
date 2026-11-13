/**
 * Read-only GitHub gate for .github/workflows/ingest-admission-renewal.yml (dormant admission renewal). Mirrors
 * the generation-overlap and schema release gates: exact live main head, first attempt, the pinned human
 * dispatcher (strict: no environment-reviewer substitute), the protected session-schema-production environment
 * (main only) and green push CI at the SHA (ci `checks` + `session-read-worker / worker`, pg-proof
 * `real PostgreSQL 15/16/17`). Renew also requires a successful same-SHA plan run of this workflow under 24h old
 * holding exactly one unexpired plan artifact, and a confirmation shaped for this project, SHA and plan run (the
 * database step then checks it byte for byte against the downloaded artifact). Errors carry fixed codes only.
 */
import { PRIVATE_INGEST_RELEASE } from "../src/session-ingest/private-ingest-release.ts";
import { SCHEMA_RELEASE as S } from "../src/hoopdb/schema-release-contract.ts";
import {
  admissionRenewalDispatch,
  refuseAdmissionRenewal as refuse,
} from "../src/hoopdb/ingest-admission-renewal-operator.ts";
import {
  ADMISSION_RENEWAL_ENABLED,
  ADMISSION_RENEWAL_PLAN_ARTIFACT,
  ADMISSION_RENEWAL_PLAN_MAX_AGE_MS,
  ADMISSION_RENEWAL_WORKFLOW,
} from "../src/hoopdb/admission-renewal-contract.ts";
import {
  githubGet,
  isPinnedDispatcher,
  requireGreenPushRuns,
  requireHumanApproval,
  requireProtectedEnvironment,
  type Get,
} from "./schema-release-github.ts";

type Obj = Record<string, any>;

export async function admissionRenewalGithubGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
  options: { enabled?: boolean; now?: number } = {},
) {
  const input = admissionRenewalDispatch(env, options.enabled ?? ADMISSION_RENEWAL_ENABLED);
  const now = options.now ?? Date.now();
  if (env.GITHUB_REPOSITORY !== S.repository || env.GITHUB_REPOSITORY_ID !== PRIVATE_INGEST_RELEASE.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" ||
      !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    refuse("admission_renewal_gate_context");
  if (input.mode === "renew" && !(input.confirmation.startsWith(`RENEW-ADMISSION ${S.project} ${input.sha} GENERATION `) &&
      input.confirmation.endsWith(` PLAN ${input.planRun}`)))
    refuse("admission_renewal_confirmation");
  const base = `/repos/${S.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha) refuse("admission_renewal_gate_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== ADMISSION_RENEWAL_WORKFLOW || run.run_attempt !== 1 || !isPinnedDispatcher(run))
    refuse("admission_renewal_gate_run");
  const gate = await requireProtectedEnvironment(get, base, S.environment, true, {
    environment: "admission_renewal_gate_environment",
    branch: "admission_renewal_gate_environment_branch",
  });
  await requireGreenPushRuns(get, base, input.sha, {
    runs: "admission_renewal_gate_ci",
    jobs: "admission_renewal_gate_ci_jobs",
  });
  if (input.mode === "renew") {
    const plan = await get(base + "/actions/runs/" + input.planRun);
    const created = Date.parse(plan.created_at);
    if (String(plan.id) !== input.planRun || String(plan.id) === env.GITHUB_RUN_ID ||
        plan.path !== ADMISSION_RENEWAL_WORKFLOW || plan.head_sha !== input.sha || plan.head_branch !== "main" ||
        plan.event !== "workflow_dispatch" || plan.run_attempt !== 1 || plan.status !== "completed" ||
        plan.conclusion !== "success" || !isPinnedDispatcher(plan) || !Number.isFinite(created) ||
        now - created >= ADMISSION_RENEWAL_PLAN_MAX_AGE_MS || created > now)
      refuse("admission_renewal_gate_plan_run");
    const artifacts = await get(base + "/actions/runs/" + input.planRun + "/artifacts?per_page=100");
    const named = (artifacts.artifacts ?? []).filter((a: Obj) => a.name === ADMISSION_RENEWAL_PLAN_ARTIFACT);
    if (named.length !== 1 || named[0].expired !== false) refuse("admission_renewal_gate_plan_artifact");
  }
  if (approved)
    await requireHumanApproval(get, base, env.GITHUB_RUN_ID!, S.environment, gate, true, {
      approval: "admission_renewal_gate_human_approval",
      rejected: "admission_renewal_gate_human_rejected",
      dispatch: "admission_renewal_gate_human_dispatch",
    });
  return input;
}

if (import.meta.main)
  admissionRenewalGithubGate(process.env, githubGet(), process.argv.includes("--approved"))
    .then(() => console.log("admission_renewal_github_gate_ok"))
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("admission_renewal_github_gate_refused " +
        (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
