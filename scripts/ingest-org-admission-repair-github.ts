/**
 * Read-only GitHub gate for .github/workflows/ingest-org-admission-repair.yml (dormant org admission repair). The
 * admission renewal gate's checks, unchanged: exact live main head, first attempt, the pinned human dispatcher
 * (strict: no environment-reviewer substitute), the protected session-schema-production environment (main only) and
 * green push CI at the SHA. Admit also requires a successful same-SHA plan run of this workflow under 24h old holding
 * exactly one unexpired plan artifact, and a confirmation shaped for this project, SHA and plan run (the database
 * step then checks it byte for byte against the downloaded artifact and the org list). Errors carry fixed codes only.
 */
import { PRIVATE_INGEST_RELEASE } from "../src/session-ingest/private-ingest-release.ts";
import { SCHEMA_RELEASE as S } from "../src/hoopdb/schema-release-contract.ts";
import {
  orgAdmissionRepairDispatch,
  orgAdmissionRepairEnv,
  orgAdmissionRepairMasks,
  refuseOrgAdmissionRepair as refuse,
} from "../src/hoopdb/ingest-org-admission-repair-operator.ts";
import {
  ORG_ADMISSION_REPAIR_ENABLED,
  ORG_ADMISSION_REPAIR_PLAN_ARTIFACT,
  ORG_ADMISSION_REPAIR_PLAN_MAX_AGE_MS,
  ORG_ADMISSION_REPAIR_WORKFLOW,
} from "../src/hoopdb/org-admission-repair-contract.ts";
import {
  githubGet,
  isPinnedDispatcher,
  requireGreenPushRuns,
  requireHumanApproval,
  requireProtectedEnvironment,
  type Get,
} from "./schema-release-github.ts";

type Obj = Record<string, any>;

export async function orgAdmissionRepairGithubGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
  options: { enabled?: boolean; now?: number } = {},
) {
  const input = orgAdmissionRepairDispatch(env, options.enabled ?? ORG_ADMISSION_REPAIR_ENABLED);
  const now = options.now ?? Date.now();
  if (env.GITHUB_REPOSITORY !== S.repository || env.GITHUB_REPOSITORY_ID !== PRIVATE_INGEST_RELEASE.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" ||
      !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    refuse("org_admission_repair_gate_context");
  if (input.mode === "admit" && !(input.confirmation.startsWith(`ADMIT-ORGS ${S.project} ${input.sha} GENERATION `) &&
      input.confirmation.endsWith(` COUNT ${input.orgs.length} PLAN ${input.planRun}`)))
    refuse("org_admission_repair_confirmation");
  const base = `/repos/${S.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha) refuse("org_admission_repair_gate_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== ORG_ADMISSION_REPAIR_WORKFLOW || run.run_attempt !== 1 || !isPinnedDispatcher(run))
    refuse("org_admission_repair_gate_run");
  const gate = await requireProtectedEnvironment(get, base, S.environment, true, {
    environment: "org_admission_repair_gate_environment",
    branch: "org_admission_repair_gate_environment_branch",
  });
  await requireGreenPushRuns(get, base, input.sha, {
    runs: "org_admission_repair_gate_ci",
    jobs: "org_admission_repair_gate_ci_jobs",
  });
  if (input.mode === "admit") {
    const plan = await get(base + "/actions/runs/" + input.planRun);
    const created = Date.parse(plan.created_at);
    if (String(plan.id) !== input.planRun || String(plan.id) === env.GITHUB_RUN_ID ||
        plan.path !== ORG_ADMISSION_REPAIR_WORKFLOW || plan.head_sha !== input.sha || plan.head_branch !== "main" ||
        plan.event !== "workflow_dispatch" || plan.run_attempt !== 1 || plan.status !== "completed" ||
        plan.conclusion !== "success" || !isPinnedDispatcher(plan) || !Number.isFinite(created) ||
        now - created >= ORG_ADMISSION_REPAIR_PLAN_MAX_AGE_MS || created > now)
      refuse("org_admission_repair_gate_plan_run");
    const artifacts = await get(base + "/actions/runs/" + input.planRun + "/artifacts?per_page=100");
    const named = (artifacts.artifacts ?? []).filter((a: Obj) => a.name === ORG_ADMISSION_REPAIR_PLAN_ARTIFACT);
    if (named.length !== 1 || named[0].expired !== false) refuse("org_admission_repair_gate_plan_artifact");
  }
  if (approved)
    await requireHumanApproval(get, base, env.GITHUB_RUN_ID!, S.environment, gate, true, {
      approval: "org_admission_repair_gate_human_approval",
      rejected: "org_admission_repair_gate_human_rejected",
      dispatch: "org_admission_repair_gate_human_dispatch",
    });
  return input;
}

if (import.meta.main)
  Promise.resolve()
    .then(() => {
      // The org list comes from the dispatch event payload (never step env); mask every id for the rest of the job.
      const env = orgAdmissionRepairEnv(process.env);
      console.log(orgAdmissionRepairMasks(orgAdmissionRepairDispatch(env, true).orgs));
      return orgAdmissionRepairGithubGate(env, githubGet(), process.argv.includes("--approved"));
    })
    .then(() => console.log("org_admission_repair_github_gate_ok"))
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("org_admission_repair_github_gate_refused " +
        (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
