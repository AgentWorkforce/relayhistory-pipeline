/**
 * Read-only GitHub gate for .github/workflows/ledger-log-pilot.yml (the dormant production pilot of one ledger-log
 * stream; docs/design/s3-ledger-log-commit.md "Production pilot"). The org admission repair gate's checks: exact live
 * main head, first attempt, the pinned human dispatcher, the run's own name, the protected ledger-log-pilot environment
 * (main only) and green push CI at the SHA. switch and close also require a successful same-SHA plan-<action> run of
 * this workflow, younger than the plan TTL, holding exactly one unexpired plan artifact, and a confirmation shaped for
 * this SHA and plan run (the operation then rebuilds it byte for byte from its own preflight). Fixed codes only.
 */
import { appendFileSync } from "node:fs";
import { PRIVATE_INGEST_RELEASE } from "../src/session-ingest/private-ingest-release.ts";
import { SCHEMA_RELEASE as S } from "../src/hoopdb/schema-release-contract.ts";
import {
  isPilotApply,
  LEDGER_LOG_PILOT_ENVIRONMENT,
  LEDGER_LOG_PILOT_PLAN_ARTIFACT,
  LEDGER_LOG_PILOT_WORKFLOW,
  LEDGER_LOG_PRODUCTION_ENABLED,
  LEDGER_LOG_PRODUCTION_PLAN_TTL_MS,
  ledgerLogPilotDispatch,
  ledgerLogPilotRunName,
  pilotAction,
} from "../src/session-ingest/ledger-log-stream-production.ts";
import {
  githubGet,
  isPinnedDispatcher,
  requireGreenPushRuns,
  requireHumanApproval,
  requireProtectedEnvironment,
  type Get,
} from "./schema-release-github.ts";

type Obj = Record<string, any>;
const refuse = (code: string): never => { throw Object.assign(new Error(code), { code }); };

export async function ledgerLogPilotGithubGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
  options: { enabled?: boolean; now?: number } = {},
) {
  const input = ledgerLogPilotDispatch(env, options.enabled ?? LEDGER_LOG_PRODUCTION_ENABLED);
  const now = options.now ?? Date.now();
  if (env.GITHUB_REPOSITORY !== S.repository || env.GITHUB_REPOSITORY_ID !== PRIVATE_INGEST_RELEASE.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" ||
      !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    refuse("ledger_log_pilot_gate_context");
  const base = `/repos/${S.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha) refuse("ledger_log_pilot_gate_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== LEDGER_LOG_PILOT_WORKFLOW || run.run_attempt !== 1 || !isPinnedDispatcher(run) ||
      run.display_title !== ledgerLogPilotRunName(input.mode, input.sha))
    refuse("ledger_log_pilot_gate_run");
  const gate = await requireProtectedEnvironment(get, base, LEDGER_LOG_PILOT_ENVIRONMENT, true, {
    environment: "ledger_log_pilot_gate_environment",
    branch: "ledger_log_pilot_gate_environment_branch",
  });
  await requireGreenPushRuns(get, base, input.sha, { runs: "ledger_log_pilot_gate_ci", jobs: "ledger_log_pilot_gate_ci_jobs" });
  if (isPilotApply(input.mode)) {
    const plan = await get(base + "/actions/runs/" + input.planRun);
    const created = Date.parse(plan.created_at);
    if (String(plan.id) !== input.planRun || String(plan.id) === env.GITHUB_RUN_ID ||
        plan.path !== LEDGER_LOG_PILOT_WORKFLOW || plan.head_sha !== input.sha || plan.head_branch !== "main" ||
        plan.event !== "workflow_dispatch" || plan.run_attempt !== 1 || plan.status !== "completed" ||
        plan.conclusion !== "success" || !isPinnedDispatcher(plan) ||
        plan.display_title !== ledgerLogPilotRunName(`plan-${pilotAction(input.mode)}`, input.sha) ||
        !Number.isFinite(created) || created > now || now - created >= LEDGER_LOG_PRODUCTION_PLAN_TTL_MS)
      refuse("ledger_log_pilot_gate_plan_run");
    const artifacts = await get(base + "/actions/runs/" + input.planRun + "/artifacts?per_page=100");
    const named = (artifacts.artifacts ?? []).filter((a: Obj) => a.name === LEDGER_LOG_PILOT_PLAN_ARTIFACT);
    if (named.length !== 1 || named[0].expired !== false) refuse("ledger_log_pilot_gate_plan_artifact");
  }
  if (approved)
    await requireHumanApproval(get, base, env.GITHUB_RUN_ID!, LEDGER_LOG_PILOT_ENVIRONMENT, gate, true, {
      approval: "ledger_log_pilot_gate_human_approval",
      rejected: "ledger_log_pilot_gate_human_rejected",
      dispatch: "ledger_log_pilot_gate_human_dispatch",
    });
  return input;
}

if (import.meta.main)
  ledgerLogPilotGithubGate(process.env, githubGet(), process.argv.includes("--approved"))
    .then(() => {
      console.log("ledger_log_pilot_github_gate_ok");
      if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `environment=${LEDGER_LOG_PILOT_ENVIRONMENT}\n`);
    })
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("ledger_log_pilot_github_gate_refused " +
        (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
