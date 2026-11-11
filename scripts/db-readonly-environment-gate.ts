/**
 * Read-only GitHub gate for .github/workflows/db-readonly-checks.yml, run in a job with no environment
 * before the gated job (the apply-session-schema.yml prerequisite-gate pattern; checks modelled on
 * scripts/schema-release-github.ts, which is bound to the schema release and not reused here).
 *
 * Referencing an environment that does not exist makes GitHub create it without protection. So before
 * the gated job is allowed to request it, this refuses unless the run is a workflow_dispatch from main and
 * the named environment exists with at least one required User reviewer and a deployment-branch policy of
 * exactly `main`. Output is a fixed code only.
 *
 * Usage: bun scripts/db-readonly-environment-gate.ts <environment>   (GITHUB_TOKEN with actions: read)
 */
type Obj = Record<string, any>;
export type Get = (path: string) => Promise<Obj>;
export const GATED_ENVIRONMENTS = ["production-db-readonly"] as const;

export class GateRefusal extends Error {
  override name = "GateRefusal";
  constructor(readonly code: string) {
    super(`refused: ${code}`);
  }
}
const refuse = (code: string): never => {
  throw new GateRefusal(code);
};

export async function environmentGate(env: Record<string, string | undefined>, name: string, get: Get): Promise<void> {
  if (!(GATED_ENVIRONMENTS as readonly string[]).includes(name)) refuse("environment_name");
  const repo = env.GITHUB_REPOSITORY ?? "";
  if (
    !/^[A-Za-z0-9_.-]{1,100}\/[A-Za-z0-9_.-]{1,100}$/.test(repo) ||
    env.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
    env.GITHUB_REF !== "refs/heads/main"
  )
    refuse("context");
  const base = `/repos/${repo}/environments/${name}`;
  const environment = await get(base);
  const reviewers =
    (environment.protection_rules ?? [])
      .find((r: Obj) => r.type === "required_reviewers")
      ?.reviewers?.filter((r: Obj) => r.type === "User" && r.reviewer?.type === "User") ?? [];
  if (
    environment.name !== name ||
    !reviewers.length ||
    environment.deployment_branch_policy?.protected_branches !== false ||
    environment.deployment_branch_policy?.custom_branch_policies !== true
  )
    refuse("environment_protection");
  const branches = await get(`${base}/deployment-branch-policies?per_page=100`);
  if (
    branches.total_count !== 1 ||
    branches.branch_policies?.length !== 1 ||
    branches.branch_policies[0].name !== "main" ||
    branches.branch_policies[0].type !== "branch"
  )
    refuse("environment_branch");
}

export function githubGet(token = process.env.GITHUB_TOKEN): Get {
  if (!token) refuse("github_token");
  return async (path) => {
    const response = await fetch("https://api.github.com" + path, {
      headers: { Authorization: "Bearer " + token, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" },
      signal: AbortSignal.timeout(15000),
      redirect: "error",
    });
    if (!response.ok) refuse("github_api");
    return (await response.json()) as Obj;
  };
}

if (import.meta.main) {
  try {
    await environmentGate(process.env, process.argv[2] ?? "", githubGet());
    console.log("db_readonly_environment_gate_ok");
  } catch (e) {
    console.log(`db_readonly_environment_gate_refused ${e instanceof GateRefusal ? e.code : "error"}`);
    process.exitCode = 1;
  }
}
