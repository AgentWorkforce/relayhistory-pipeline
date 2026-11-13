/**
 * Read-only GitHub gate for .github/workflows/index-prebuild-058000.yml (the 058000 CONCURRENTLY index prebuild:
 * census, dormant prebuild, readback). The same gates as the guarded schema release and its operators
 * (scripts/schema-release-github.ts): repository id, main ref, the exact live main head, first attempt, the pinned
 * human dispatcher on both actor and triggering_actor (strict: no environment-reviewer substitute), the protected
 * session-schema-production environment (main only), green push CI and pg-proof at the SHA, and the recorded
 * environment approval inside the protected job. Prebuild also requires a successful same-SHA census run of this
 * workflow under 24h old, by the same pinned dispatcher, holding exactly one unexpired census artifact, and a
 * confirmation shaped for this SHA and census run (the database step then checks it byte for byte against the
 * downloaded artifact). The dormant switch and the inputs are checked first, before any GitHub read. Errors carry
 * fixed codes only.
 */
import { PRIVATE_INGEST_RELEASE } from "../src/session-ingest/private-ingest-release.ts";
import { SCHEMA_RELEASE as S } from "../src/hoopdb/schema-release-contract.ts";
import {
  INDEX_PREBUILD_CENSUS_ARTIFACT,
  INDEX_PREBUILD_CENSUS_MAX_AGE_MS,
  INDEX_PREBUILD_ENABLED,
  INDEX_PREBUILD_WORKFLOW,
  indexPrebuildDispatch,
  refuseIndexPrebuild as refuse,
} from "../src/hoopdb/index-prebuild-058000.ts";
import {
  githubGet,
  isPinnedDispatcher,
  requireGreenPushRuns,
  requireHumanApproval,
  requireProtectedEnvironment,
  type Get,
} from "./schema-release-github.ts";

type Obj = Record<string, any>;

export async function indexPrebuildGithubGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
  options: { enabled?: boolean; now?: number } = {},
) {
  const input = indexPrebuildDispatch(env, options.enabled ?? INDEX_PREBUILD_ENABLED);
  const now = options.now ?? Date.now();
  if (env.GITHUB_REPOSITORY !== S.repository || env.GITHUB_REPOSITORY_ID !== PRIVATE_INGEST_RELEASE.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" ||
      !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    refuse("index_prebuild_gate_context");
  if (input.mode === "prebuild" &&
      !(input.confirmation.startsWith(`PREBUILD-058000 ${input.sha} GENERATION `) &&
        input.confirmation.includes(` CENSUS ${input.censusRun} INDEXES `)))
    refuse("index_prebuild_confirmation");
  const base = `/repos/${S.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha) refuse("index_prebuild_gate_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== INDEX_PREBUILD_WORKFLOW || run.run_attempt !== 1 || !isPinnedDispatcher(run))
    refuse("index_prebuild_gate_run");
  const gate = await requireProtectedEnvironment(get, base, S.environment, true, {
    environment: "index_prebuild_gate_environment",
    branch: "index_prebuild_gate_environment_branch",
  });
  // The schema release's own required push runs: ci (checks, session-read-worker) and pg-proof 15/16/17.
  await requireGreenPushRuns(get, base, input.sha, {
    runs: "index_prebuild_gate_ci",
    jobs: "index_prebuild_gate_ci_jobs",
  });
  if (input.mode === "prebuild") {
    const census = await get(base + "/actions/runs/" + input.censusRun);
    const created = Date.parse(census.created_at);
    if (String(census.id) !== input.censusRun || String(census.id) === env.GITHUB_RUN_ID ||
        census.path !== INDEX_PREBUILD_WORKFLOW || census.head_sha !== input.sha || census.head_branch !== "main" ||
        census.event !== "workflow_dispatch" || census.run_attempt !== 1 || census.status !== "completed" ||
        census.conclusion !== "success" || !isPinnedDispatcher(census) || !Number.isFinite(created) ||
        now - created >= INDEX_PREBUILD_CENSUS_MAX_AGE_MS || created > now)
      refuse("index_prebuild_gate_census_run");
    const artifacts = await get(base + "/actions/runs/" + input.censusRun + "/artifacts?per_page=100");
    const named = (artifacts.artifacts ?? []).filter((a: Obj) => a.name === INDEX_PREBUILD_CENSUS_ARTIFACT);
    if (named.length !== 1 || named[0].expired !== false) refuse("index_prebuild_gate_census_artifact");
  }
  if (approved)
    await requireHumanApproval(get, base, env.GITHUB_RUN_ID!, S.environment, gate, true, {
      approval: "index_prebuild_gate_human_approval",
      rejected: "index_prebuild_gate_human_rejected",
      dispatch: "index_prebuild_gate_human_dispatch",
    });
  return input;
}

if (import.meta.main)
  indexPrebuildGithubGate(process.env, githubGet(), process.argv.includes("--approved"))
    .then(() => console.log("index_prebuild_github_gate_ok"))
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("index_prebuild_github_gate_refused " +
        (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
