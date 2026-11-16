/**
 * Read-only GitHub gate for .github/workflows/ingest-error-alert.yml (ingest DB-error alert). Mirrors the admission
 * renewal gate: exact live main head, first attempt, the pinned human dispatcher, the protected
 * `ingest-alerting-production` environment (main only) and green push CI at the SHA. With --approved (inside the
 * environment job) it also requires the recorded environment approval. Apply additionally needs a confirmation
 * shaped for this SHA; the alerting job then checks its body digest against the re-planned body. Errors carry fixed
 * codes only.
 *
 * trustedDispatcher is true on purpose, as in the schema release and admission renewal gates: the repository plan
 * cannot hold required environment reviewers (GitHub 422, runbooks/guarded-session-schema-release.md "Operator
 * read-only plan while reviewer billing is blocked"). With a required_reviewers rule present, a recorded approval
 * by one of its User reviewers is required and any rejection refuses; without one, the human gate is the
 * first-attempt dispatch by Khaliq's own pinned User account, which the run check above always requires anyway.
 */
import { PRIVATE_INGEST_RELEASE } from "../src/session-ingest/private-ingest-release.ts";
import { SCHEMA_RELEASE as S } from "../src/hoopdb/schema-release-contract.ts";
import {
  INGEST_ERROR_ALERT as A,
  ingestErrorAlertDispatch,
  refuseIngestErrorAlert as refuse,
} from "../src/session-ingest/ingest-error-alert.ts";
import {
  githubGet,
  isPinnedDispatcher,
  requireGreenPushRuns,
  requireHumanApproval,
  requireProtectedEnvironment,
  type Get,
} from "./schema-release-github.ts";

export async function ingestErrorAlertGithubGate(env: Record<string, string | undefined>, get: Get, approved = false) {
  const input = ingestErrorAlertDispatch(env);
  if (env.GITHUB_REPOSITORY !== S.repository || env.GITHUB_REPOSITORY_ID !== PRIVATE_INGEST_RELEASE.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" || !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    refuse("ingest_error_alert_gate_context");
  if (input.mode === "apply" && !input.confirmation.startsWith(`APPLY-INGEST-DB-ERROR-ALERT ${input.sha} BODY `))
    refuse("ingest_error_alert_confirmation");
  const base = `/repos/${S.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha) refuse("ingest_error_alert_gate_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== A.workflow || run.run_attempt !== 1 || !isPinnedDispatcher(run))
    refuse("ingest_error_alert_gate_run");
  const gate = await requireProtectedEnvironment(get, base, A.environment, true, {
    environment: "ingest_error_alert_gate_environment",
    branch: "ingest_error_alert_gate_environment_branch",
  });
  await requireGreenPushRuns(get, base, input.sha, {
    runs: "ingest_error_alert_gate_ci",
    jobs: "ingest_error_alert_gate_ci_jobs",
  });
  if (approved)
    await requireHumanApproval(get, base, env.GITHUB_RUN_ID!, A.environment, gate, true, {
      approval: "ingest_error_alert_gate_human_approval",
      rejected: "ingest_error_alert_gate_human_rejected",
      dispatch: "ingest_error_alert_gate_human_dispatch",
    });
  return input;
}

if (import.meta.main)
  ingestErrorAlertGithubGate(process.env, githubGet(), process.argv.includes("--approved"))
    .then(() => console.log("ingest_error_alert_github_gate_ok"))
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("ingest_error_alert_github_gate_refused " +
        (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
