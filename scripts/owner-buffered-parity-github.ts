/**
 * Read-only GitHub gate for .github/workflows/owner-buffered-parity.yml (exact-seven owner-buffered parity
 * census, dormant G2 write, readback). Mirrors the admission-renewal gate: repository id, main ref, the exact
 * live main head, first attempt, the pinned human dispatcher (strict: no environment-reviewer substitute), the
 * protected session-schema-production environment (main only) and green push CI at the SHA (ci `checks`,
 * pg-proof `real PostgreSQL 15/16/17`). Write also requires a successful same-SHA census run of this workflow
 * under 24h old holding exactly one unexpired census artifact, and a confirmation shaped for this SHA, source
 * and census run (the database step then checks it byte for byte against the downloaded artifact). The dormant
 * switch is checked first. Errors carry fixed codes only.
 */
import { PRIVATE_INGEST_RELEASE } from "../src/session-ingest/private-ingest-release.ts";
import { SCHEMA_RELEASE as S } from "../src/hoopdb/schema-release-contract.ts";
import {
  OWNER_PARITY_CENSUS_ARTIFACT,
  OWNER_PARITY_CENSUS_MAX_AGE_MS,
  OWNER_PARITY_WORKFLOW,
  OWNER_PARITY_WRITE_ENABLED,
  ownerParityDispatch,
  refuseOwnerParity as refuse,
} from "../src/migration/owner-buffered-parity-ci-contract.ts";
import {
  githubGet,
  isPinnedDispatcher,
  requireGreenPushRuns,
  requireHumanApproval,
  requireProtectedEnvironment,
  type Get,
} from "./schema-release-github.ts";

type Obj = Record<string, any>;

/** Exactly the brief's required push checks (not the default list, which also names session-read-worker). */
export const OWNER_PARITY_REQUIRED_PUSH_RUNS = [
  ["ci.yml", ["checks"]],
  ["pg-proof.yml", ["real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]],
] as const;

export async function ownerParityGithubGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
  options: { enabled?: boolean; now?: number } = {},
) {
  const input = ownerParityDispatch(env, options.enabled ?? OWNER_PARITY_WRITE_ENABLED);
  const now = options.now ?? Date.now();
  if (env.GITHUB_REPOSITORY !== S.repository || env.GITHUB_REPOSITORY_ID !== PRIVATE_INGEST_RELEASE.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" ||
      !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    refuse("owner_parity_gate_context");
  if (input.mode === "write" &&
      !(input.confirmation.startsWith(`WRITE-OWNER-PARITY ${input.sha} GENERATION `) &&
        input.confirmation.includes(` SOURCE ${input.censusSource} DESCRIPTOR `) &&
        input.confirmation.endsWith(` CENSUS ${input.censusRun}`)))
    refuse("owner_parity_confirmation");
  const base = `/repos/${S.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha) refuse("owner_parity_gate_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== OWNER_PARITY_WORKFLOW || run.run_attempt !== 1 || !isPinnedDispatcher(run))
    refuse("owner_parity_gate_run");
  const gate = await requireProtectedEnvironment(get, base, S.environment, true, {
    environment: "owner_parity_gate_environment",
    branch: "owner_parity_gate_environment_branch",
  });
  await requireGreenPushRuns(get, base, input.sha, {
    runs: "owner_parity_gate_ci",
    jobs: "owner_parity_gate_ci_jobs",
  }, "main", OWNER_PARITY_REQUIRED_PUSH_RUNS);
  if (input.mode === "write") {
    const census = await get(base + "/actions/runs/" + input.censusRun);
    const created = Date.parse(census.created_at);
    if (String(census.id) !== input.censusRun || String(census.id) === env.GITHUB_RUN_ID ||
        census.path !== OWNER_PARITY_WORKFLOW || census.head_sha !== input.sha || census.head_branch !== "main" ||
        census.event !== "workflow_dispatch" || census.run_attempt !== 1 || census.status !== "completed" ||
        census.conclusion !== "success" || !isPinnedDispatcher(census) || !Number.isFinite(created) ||
        now - created >= OWNER_PARITY_CENSUS_MAX_AGE_MS || created > now)
      refuse("owner_parity_gate_census_run");
    const artifacts = await get(base + "/actions/runs/" + input.censusRun + "/artifacts?per_page=100");
    const named = (artifacts.artifacts ?? []).filter((a: Obj) => a.name === OWNER_PARITY_CENSUS_ARTIFACT);
    if (named.length !== 1 || named[0].expired !== false) refuse("owner_parity_gate_census_artifact");
  }
  if (approved)
    await requireHumanApproval(get, base, env.GITHUB_RUN_ID!, S.environment, gate, true, {
      approval: "owner_parity_gate_human_approval",
      rejected: "owner_parity_gate_human_rejected",
      dispatch: "owner_parity_gate_human_dispatch",
    });
  return input;
}

if (import.meta.main)
  ownerParityGithubGate(process.env, githubGet(), process.argv.includes("--approved"))
    .then(() => console.log("owner_parity_github_gate_ok"))
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("owner_parity_github_gate_refused " +
        (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
