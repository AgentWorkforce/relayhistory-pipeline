/**
 * Read-only GitHub gate for .github/workflows/backlog-ops.yml (the 0005 backlog tools against production; contract in
 * src/session-ingest/backlog-ops.ts). The same gates as the guarded schema release and its operators
 * (scripts/schema-release-github.ts): repository id, main ref, the exact live main head, first attempt, the pinned
 * human dispatcher on both actor and triggering_actor, the protected session-schema-production environment (main
 * only), green push CI and pg-proof at the SHA, and, inside the protected job, the recorded environment approval.
 * An execute also binds earlier runs of this workflow at the same SHA: each completed/success, first attempt, by the
 * pinned dispatcher, of the expected mode (read back from its run-name), under 24 h old, holding exactly one
 * unexpired receipt artifact; the drain's backfill under BACKLOG_OPS_FRESH_BACKFILL_MS old and its export created
 * after that backfill. The dormant switch and the inputs are checked first, before any GitHub read. Errors carry
 * fixed codes only.
 */
import { PRIVATE_INGEST_RELEASE } from "../src/session-ingest/private-ingest-release.ts";
import { SCHEMA_RELEASE as S } from "../src/hoopdb/schema-release-contract.ts";
import {
  BACKLOG_OPS_BINDING_MAX_AGE_MS,
  BACKLOG_OPS_EXECUTE_ENABLED,
  BACKLOG_OPS_FRESH_BACKFILL_MS,
  BACKLOG_OPS_RECEIPT_ARTIFACT,
  BACKLOG_OPS_WORKFLOW,
  backlogOpsBindings,
  backlogOpsDispatch,
  backlogOpsRunName,
  refuseBacklogOps as refuse,
} from "../src/session-ingest/backlog-ops.ts";
import {
  githubGet,
  isPinnedDispatcher,
  requireGreenPushRuns,
  requireHumanApproval,
  requireProtectedEnvironment,
  type Get,
} from "./schema-release-github.ts";

type Obj = Record<string, any>;

export async function backlogOpsGithubGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
  options: { enabled?: boolean; now?: number } = {},
) {
  const input = backlogOpsDispatch(env, options.enabled ?? BACKLOG_OPS_EXECUTE_ENABLED);
  const now = options.now ?? Date.now();
  if (env.GITHUB_REPOSITORY !== S.repository || env.GITHUB_REPOSITORY_ID !== PRIVATE_INGEST_RELEASE.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" ||
      !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    refuse("backlog_ops_gate_context");
  const base = `/repos/${S.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha) refuse("backlog_ops_gate_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== BACKLOG_OPS_WORKFLOW || run.run_attempt !== 1 || !isPinnedDispatcher(run) ||
      run.display_title !== backlogOpsRunName(input.mode, input.sha))
    refuse("backlog_ops_gate_run");
  const gate = await requireProtectedEnvironment(get, base, S.environment, true, {
    environment: "backlog_ops_gate_environment",
    branch: "backlog_ops_gate_environment_branch",
  });
  await requireGreenPushRuns(get, base, input.sha, { runs: "backlog_ops_gate_ci", jobs: "backlog_ops_gate_ci_jobs" });
  const created: Record<string, number> = {};
  for (const b of backlogOpsBindings(input)) {
    const bound = await get(base + "/actions/runs/" + b.run);
    const at = Date.parse(bound.created_at);
    const maxAge = b.kind === "backfill" && input.mode === "drain-execute" ? BACKLOG_OPS_FRESH_BACKFILL_MS : BACKLOG_OPS_BINDING_MAX_AGE_MS;
    if (String(bound.id) !== b.run || String(bound.id) === env.GITHUB_RUN_ID || bound.path !== BACKLOG_OPS_WORKFLOW ||
        bound.head_sha !== input.sha || bound.head_branch !== "main" || bound.event !== "workflow_dispatch" ||
        bound.run_attempt !== 1 || bound.status !== "completed" || bound.conclusion !== "success" ||
        !isPinnedDispatcher(bound) || bound.display_title !== backlogOpsRunName(b.mode, input.sha) ||
        !Number.isFinite(at) || at > now || now - at >= maxAge)
      refuse(`backlog_ops_gate_${b.kind}_run`);
    const artifacts = await get(base + "/actions/runs/" + b.run + "/artifacts?per_page=100");
    const named = (artifacts.artifacts ?? []).filter((a: Obj) => a.name === BACKLOG_OPS_RECEIPT_ARTIFACT);
    if (named.length !== 1 || named[0].expired !== false) refuse(`backlog_ops_gate_${b.kind}_artifact`);
    created[b.kind] = at;
  }
  // The drain's export ran after its backfill (the export reads the fences the backfill wrote).
  if (input.mode === "drain-execute" && !(created.export! > created.backfill!)) refuse("backlog_ops_gate_order");
  if (approved)
    await requireHumanApproval(get, base, env.GITHUB_RUN_ID!, S.environment, gate, true, {
      approval: "backlog_ops_gate_human_approval",
      rejected: "backlog_ops_gate_human_rejected",
      dispatch: "backlog_ops_gate_human_dispatch",
    });
  return input;
}

if (import.meta.main)
  backlogOpsGithubGate(process.env, githubGet(), process.argv.includes("--approved"))
    .then(() => console.log("backlog_ops_github_gate_ok"))
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("backlog_ops_github_gate_refused " +
        (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
