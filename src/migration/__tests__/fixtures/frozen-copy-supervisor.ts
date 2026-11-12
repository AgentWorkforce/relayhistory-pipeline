// Synthetic supervisor fixture: a fake GitHub API for the custody workflow (all ids, shas and logins are fake except
// the pinned repository / workflow / dispatcher constants, which are the values under test). No network.
import {
  FROZEN_CUSTODY, approvalText, frozenCopyOperatorSha256, sourceDescriptorSha256, targetDescriptorSha256, verifySupervisor,
  type CustodyAction, type SupervisorFacts, type SupervisorProof,
} from "../../frozen-source-copy-supervisor.ts";

const H = (c: string) => c.repeat(64);
export const FIXTURE_POLICY = { generation: H("c"), source_identity_sha256: H("d"), control_epoch: H("a"), privacy_baseline_sha256: H("b") };
export const FIXTURE_STATUS = { phase: "active", epoch: FIXTURE_POLICY.control_epoch, privacyBaselineSha256: FIXTURE_POLICY.privacy_baseline_sha256 };
export const SOURCE_SHA = "5".repeat(40), RUN_ID = "7001", PLAN_RUN = "7000";
const ENV_ID = 4242, CI_RUN = 6001, DEPLOYMENT = 8001;
const base = `/repos/${FROZEN_CUSTODY.repository}`;
const pinned = { login: FROZEN_CUSTODY.dispatcher.login, id: FROZEN_CUSTODY.dispatcher.id, type: "User" };

export type Route = "main" | "run" | "plan" | "environment" | "branches" | "ciRuns" | "ciJobs" | "approvals" | "deployments" | "statuses";
export type Forge = Partial<Record<Route, (body: any) => any>>;

/** Answers exactly the supervisor's reads; `forge` rewrites one route's body (or throws, as an API failure). */
export function fakeGithub(forge: Forge = {}, mode: CustodyAction | "plan" = "copy", sha = SOURCE_SHA) {
  const runBody = (id: string, mode: string, created = Date.now() - 3600_000) => ({
    id: Number(id), event: "workflow_dispatch", head_branch: "main", head_sha: sha, path: FROZEN_CUSTODY.workflow,
    repository: { id: Number(FROZEN_CUSTODY.repositoryId) }, run_attempt: 1, actor: { ...pinned }, triggering_actor: { ...pinned },
    display_title: `frozen-source-custody ${mode} ${sha}`, status: "completed", conclusion: "success", created_at: new Date(created).toISOString(),
  });
  const routes: Array<[Route, string, () => any]> = [
    ["main", "/git/ref/heads/main", () => ({ object: { sha } })],
    ["run", `/actions/runs/${RUN_ID}`, () => ({ ...runBody(RUN_ID, mode, Date.now()), status: "in_progress", conclusion: null })],
    ["plan", `/actions/runs/${PLAN_RUN}`, () => runBody(PLAN_RUN, "plan")],
    ["environment", `/environments/${FROZEN_CUSTODY.environment}`, () => ({
      id: ENV_ID, name: FROZEN_CUSTODY.environment,
      protection_rules: [{ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { ...pinned } }] }],
      deployment_branch_policy: { protected_branches: false, custom_branch_policies: true },
    })],
    ["branches", `/environments/${FROZEN_CUSTODY.environment}/deployment-branch-policies?per_page=100`, () => ({ total_count: 1, branch_policies: [{ name: "main", type: "branch" }] })],
    ["ciRuns", `/actions/workflows/ci.yml/runs?event=push&head_sha=${sha}&per_page=100`, () => ({
      workflow_runs: [{ id: CI_RUN, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }],
    })],
    ["ciJobs", `/actions/runs/${CI_RUN}/attempts/1/jobs?per_page=100`, () => ({ total_count: 1, jobs: [{ name: "checks", status: "completed", conclusion: "success", head_sha: sha }] })],
    ["approvals", `/actions/runs/${RUN_ID}/approvals`, () => [{ state: "approved", user: { ...pinned }, environments: [{ id: ENV_ID, name: FROZEN_CUSTODY.environment }] }]],
    ["deployments", `/deployments?environment=${FROZEN_CUSTODY.environment}&sha=${sha}&per_page=100`, () => [{ id: DEPLOYMENT, environment: FROZEN_CUSTODY.environment, sha }]],
    ["statuses", `/deployments/${DEPLOYMENT}/statuses?per_page=100`, () => [{ state: "in_progress", log_url: `https://github.com/${FROZEN_CUSTODY.repository}/actions/runs/${RUN_ID}/job/1` }]],
  ];
  const calls: string[] = [];
  const get = async (path: string) => {
    calls.push(path);
    const r = routes.find(([, p]) => base + p === path);
    if (!r) throw Object.assign(new Error("fake github: unexpected path"), { code: "release_github_api" });
    const body = r[2]();
    return forge[r[0]] ? forge[r[0]]!(body) : body;
  };
  return Object.assign(get, { calls });
}

/** The Actions environment of the protected job (what GitHub sets, plus the workflow's inputs). */
export function supervisorEnv(mode: CustodyAction | "plan", approval = "", over: Record<string, string | undefined> = {}): Record<string, string | undefined> {
  return {
    GITHUB_ACTIONS: "true", GITHUB_TOKEN: "synthetic-github-token", GITHUB_RUN_ID: RUN_ID, GITHUB_RUN_ATTEMPT: "1",
    GITHUB_REPOSITORY: FROZEN_CUSTODY.repository, GITHUB_REPOSITORY_ID: FROZEN_CUSTODY.repositoryId, GITHUB_EVENT_NAME: "workflow_dispatch",
    GITHUB_REF: "refs/heads/main", GITHUB_WORKFLOW_REF: `${FROZEN_CUSTODY.repository}/${FROZEN_CUSTODY.workflow}@refs/heads/main`,
    GITHUB_ACTOR: FROZEN_CUSTODY.dispatcher.login, GITHUB_TRIGGERING_ACTOR: FROZEN_CUSTODY.dispatcher.login, GITHUB_SHA: SOURCE_SHA,
    CUSTODY_MODE: mode, CUSTODY_SHA: SOURCE_SHA, ...(mode === "plan" ? {} : { CUSTODY_PLAN_RUN: PLAN_RUN, CUSTODY_APPROVAL: approval }), ...over,
  };
}

/** What the driver derives against the synthetic source/target (preload FIXTURE_POLICY + FIXTURE_STATUS). */
export function fixtureFacts(action: CustodyAction, o: { page?: number; maxPages?: number; budgetMs?: number; operator?: string } = {}): SupervisorFacts {
  return {
    action, source: sourceDescriptorSha256(FIXTURE_STATUS), epoch: FIXTURE_POLICY.control_epoch, privacy: FIXTURE_POLICY.privacy_baseline_sha256,
    target: targetDescriptorSha256({ generation: FIXTURE_POLICY.generation, sourceIdentitySha256: FIXTURE_POLICY.source_identity_sha256,
      controlEpoch: FIXTURE_POLICY.control_epoch, privacyBaselineSha256: FIXTURE_POLICY.privacy_baseline_sha256 }),
    generation: FIXTURE_POLICY.generation, manifest: null, operator: o.operator ?? frozenCopyOperatorSha256(),
    page: o.page ?? 5000, maxPages: o.maxPages ?? 0, budgetMs: o.budgetMs ?? 6 * 60 * 60 * 1000,
  };
}
export const fixtureApproval = (f: SupervisorFacts, manifest: string | null = null) =>
  approvalText({ ...f, manifest, sha: SOURCE_SHA, planRun: PLAN_RUN });

/** A real proof minted through verifySupervisor against the fake GitHub (the only way to get one). */
export async function mintSupervisorProof(action: CustodyAction, o: { manifest?: string | null; epoch?: string; privacy?: string } = {}): Promise<SupervisorProof> {
  const f = { ...fixtureFacts(action), ...(o.epoch ? { epoch: o.epoch } : {}), ...(o.privacy ? { privacy: o.privacy } : {}) };
  return verifySupervisor(supervisorEnv(action, fixtureApproval(f, o.manifest ?? null)), fakeGithub({}, action), f);
}
