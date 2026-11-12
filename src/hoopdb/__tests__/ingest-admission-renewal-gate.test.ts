/**
 * GitHub gate, connection target and workflow shape for the dormant admission renewal
 * (scripts/ingest-admission-renewal-github.ts, .github/workflows/ingest-admission-renewal.yml). The production
 * project ref is only ever referenced through SCHEMA_RELEASE; it is never written here.
 */
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { admissionRenewalGithubGate } from "../../../scripts/ingest-admission-renewal-github.ts";
import { SCHEMA_RELEASE as P, releaseConnection } from "../schema-release-contract.ts";
import { releaseClient, requireReleaseOwner } from "../schema-release-inspect.ts";
import { PRIVATE_INGEST_RELEASE } from "../../session-ingest/private-ingest-release.ts";
import {
  ADMISSION_RENEWAL_ENABLED,
  ADMISSION_RENEWAL_PLAN_ARTIFACT,
  ADMISSION_RENEWAL_WORKFLOW,
} from "../admission-renewal-contract.ts";

const sha = "1".repeat(40), planRun = "424242", G = "a".repeat(64), D = "d".repeat(64), O = "c".repeat(64);
const code = async (fn: () => unknown) => {
  try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};

function githubFixture() {
  const actor = { id: P.dispatcher.id, login: P.dispatcher.login, type: "User" };
  const run = { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: ADMISSION_RENEWAL_WORKFLOW,
    run_attempt: 1, actor, triggering_actor: { ...actor }, created_at: new Date().toISOString() };
  return {
    ref: { object: { sha } }, run,
    environment: { id: 7, name: P.environment, protection_rules: [{ type: "branch_policy" }] as any[],
      deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } },
    branches: { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] },
    runs: { workflow_runs: [{ id: 10, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] },
    jobs: { total_count: 6, jobs: ["checks", "session-read-worker / worker", "unit-tests", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
      .map((name) => ({ name, head_sha: sha, status: "completed", conclusion: "success" })) },
    approvals: [] as any[],
    plan: { ...run, actor: { ...actor }, triggering_actor: { ...actor }, id: Number(planRun), status: "completed", conclusion: "success",
      created_at: new Date(Date.now() - 3600_000).toISOString() },
    artifacts: { total_count: 1, artifacts: [{ name: ADMISSION_RENEWAL_PLAN_ARTIFACT, expired: false }] as any[] },
  };
}
type Fixture = ReturnType<typeof githubFixture>;
const githubGet = (f: Fixture) => async (path: string): Promise<any> => {
  if (path.endsWith("/git/ref/heads/main")) return f.ref;
  if (path.endsWith("/actions/runs/100")) return f.run;
  if (path.endsWith("/actions/runs/100/approvals")) return f.approvals;
  if (path.endsWith(`/actions/runs/${planRun}`)) return f.plan;
  if (path.endsWith(`/actions/runs/${planRun}/artifacts?per_page=100`)) return f.artifacts;
  if (path.endsWith("/environments/" + f.environment.name)) return f.environment;
  if (path.includes("deployment-branch-policies")) return f.branches;
  if (path.includes("/actions/workflows/")) return f.runs;
  if (path.includes("/runs/10/")) return f.jobs;
  throw new Error("unexpected " + path);
};
const context = {
  GITHUB_REPOSITORY: P.repository, GITHUB_REPOSITORY_ID: PRIVATE_INGEST_RELEASE.repositoryId, GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main", GITHUB_SHA: sha, GITHUB_RUN_ATTEMPT: "1", GITHUB_RUN_ID: "100",
};
const phrase = `RENEW-ADMISSION ${P.project} ${sha} GENERATION ${G} FROM 2026-12-01T00:00:00.000Z TO 2026-12-31T00:00:00.000Z` +
  ` ROWS 6 ORGS ${O} DESCRIPTOR ${D} PLAN ${planRun}`;
const env = (mode: string, extra: Record<string, string> = {}) => ({ ...context, RENEWAL_MODE: mode, RENEWAL_SHA: sha, ...extra });
const renewEnv = env("renew", { RENEWAL_PLAN_RUN: planRun, RENEWAL_CONFIRMATION: phrase });
const gate = (e: Record<string, string>, f: Fixture, enabled = true) => code(() => admissionRenewalGithubGate(e, githubGet(f), true, { enabled }));

test("dormant gate: plan and renew refuse while disabled; readback passes every gate", async () => {
  expect(ADMISSION_RENEWAL_ENABLED).toBe(false);
  expect(await gate(env("plan"), githubFixture(), false)).toBe("admission_renewal_dormant");
  expect(await gate(renewEnv, githubFixture(), false)).toBe("admission_renewal_dormant");
  expect(await gate(env("readback"), githubFixture(), false)).toBeNull();
  expect(await gate(env("plan"), githubFixture())).toBeNull();
  expect(await gate(renewEnv, githubFixture())).toBeNull();
});

test("stale main, CI, plan, attempt, artifact, dispatcher, environment and approval refuse the gate", async () => {
  const failures: Array<[Record<string, string>, (f: Fixture) => void, string]> = [
    [renewEnv, (f) => { f.ref.object.sha = "b".repeat(40); }, "admission_renewal_gate_main_moved"],
    [{ ...renewEnv, GITHUB_SHA: "b".repeat(40) }, () => {}, "admission_renewal_gate_context"],
    [{ ...renewEnv, GITHUB_RUN_ATTEMPT: "2" }, () => {}, "admission_renewal_gate_context"],
    [{ ...renewEnv, GITHUB_REF: "refs/heads/dev" }, () => {}, "admission_renewal_gate_context"],
    [{ ...renewEnv, GITHUB_EVENT_NAME: "push" }, () => {}, "admission_renewal_gate_context"],
    [{ ...renewEnv, GITHUB_REPOSITORY_ID: "1" }, () => {}, "admission_renewal_gate_context"],
    [renewEnv, (f) => { f.run.run_attempt = 2; }, "admission_renewal_gate_run"],
    [renewEnv, (f) => { f.run.actor.id = 42; }, "admission_renewal_gate_run"],
    [renewEnv, (f) => { f.run.triggering_actor.login = "someone"; }, "admission_renewal_gate_run"],
    [renewEnv, (f) => { f.run.path = ".github/workflows/ingest-generation-overlap.yml"; }, "admission_renewal_gate_run"],
    [renewEnv, (f) => { f.environment.deployment_branch_policy.protected_branches = true; }, "admission_renewal_gate_environment"],
    [renewEnv, (f) => { f.branches.branch_policies.push({ name: "dev", type: "branch" }); f.branches.total_count = 2; }, "admission_renewal_gate_environment_branch"],
    [renewEnv, (f) => { f.runs.workflow_runs[0]!.conclusion = "failure"; }, "admission_renewal_gate_ci"],
    [renewEnv, (f) => { f.jobs.jobs = f.jobs.jobs.filter((j) => j.name !== "real PostgreSQL 16"); f.jobs.total_count = 5; }, "admission_renewal_gate_ci_jobs"],
    [renewEnv, (f) => { f.jobs.jobs[0]!.conclusion = "failure"; }, "admission_renewal_gate_ci_jobs"],
    [renewEnv, (f) => { f.plan.head_sha = "b".repeat(40); }, "admission_renewal_gate_plan_run"],
    [renewEnv, (f) => { f.plan.run_attempt = 2; }, "admission_renewal_gate_plan_run"],
    [renewEnv, (f) => { f.plan.conclusion = "failure"; }, "admission_renewal_gate_plan_run"],
    [renewEnv, (f) => { f.plan.path = ".github/workflows/ingest-generation-overlap.yml"; }, "admission_renewal_gate_plan_run"],
    [renewEnv, (f) => { f.plan.actor.id = 42; }, "admission_renewal_gate_plan_run"],
    [renewEnv, (f) => { f.plan.created_at = new Date(Date.now() - 24 * 3600_000 - 1000).toISOString(); }, "admission_renewal_gate_plan_run"],
    [renewEnv, (f) => { f.plan.created_at = new Date(Date.now() + 3600_000).toISOString(); }, "admission_renewal_gate_plan_run"],
    [{ ...renewEnv, RENEWAL_PLAN_RUN: "100", RENEWAL_CONFIRMATION: phrase.replace(`PLAN ${planRun}`, "PLAN 100") },
      (f) => { f.plan.id = 100; }, "admission_renewal_gate_plan_run"],
    [renewEnv, (f) => { f.artifacts.artifacts[0]!.expired = true; }, "admission_renewal_gate_plan_artifact"],
    [renewEnv, (f) => { f.artifacts.artifacts.push({ name: ADMISSION_RENEWAL_PLAN_ARTIFACT, expired: false }); }, "admission_renewal_gate_plan_artifact"],
    [renewEnv, (f) => { f.artifacts.artifacts = []; }, "admission_renewal_gate_plan_artifact"],
    [renewEnv, (f) => { f.environment.protection_rules.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] }); },
      "admission_renewal_gate_human_approval"],
  ];
  for (const [input, mutate, expected] of failures) {
    const f = githubFixture();
    mutate(f);
    expect(await gate(input, f)).toBe(expected);
  }
});

test("confirmation shape: project, sha and plan run are bound before any GitHub read", async () => {
  for (const bad of [phrase.replace(P.project, "fixtureprojectref000"), phrase.replace(`${sha} GENERATION`, `${"2".repeat(40)} GENERATION`),
    phrase.replace(`PLAN ${planRun}`, "PLAN 1"), phrase + " ", "renew-admission " + phrase.slice(16)])
    expect(await gate({ ...renewEnv, RENEWAL_CONFIRMATION: bad }, githubFixture())).toBe("admission_renewal_confirmation");
  expect(await gate(env("readback", { RENEWAL_CONFIRMATION: phrase }), githubFixture())).toBe("admission_renewal_confirmation");
  expect(await gate(env("renew", { RENEWAL_CONFIRMATION: phrase }), githubFixture())).toBe("admission_renewal_plan_run");
  expect(await gate(env("undo"), githubFixture())).toBe("admission_renewal_inputs");
});

test("wrong target or TLS options refuse the owner connection; the client pins verify-full TLS fields", () => {
  const user = encodeURIComponent(`${P.owner}.${P.project}`);
  const url = (o: { host?: string; port?: number; db?: string; user?: string; q?: string } = {}) =>
    `postgresql://${o.user ?? user}:pw@${o.host ?? P.host}:${o.port ?? P.port}/${o.db ?? P.database}${o.q ?? "?sslmode=verify-full"}`;
  expect(releaseConnection(url()).host).toBe(P.host);
  const refused = (u: string | undefined) => { try { releaseConnection(u); return null; } catch (e) { return (e as { code?: string }).code; } };
  for (const bad of [url({ host: "db.example.invalid" }), url({ port: 6543 }), url({ db: "other" }),
    url({ user: encodeURIComponent(`${P.owner}.fixtureprojectref000`) }), url({ user: "postgres" }), undefined, "not a url"])
    expect(refused(bad)).toBe("release_connection");
  for (const q of ["?sslmode=disable", "?sslmode=prefer", "?sslmode=verify-full&sslrootcert=/tmp/x", "?options=-csearch_path%3Dpublic"])
    expect(refused(url({ q }))).toBe("release_connection_options");
  const client = releaseClient(url()) as unknown as { connectionParameters: { ssl: { rejectUnauthorized: boolean; servername: string } } };
  expect(client.connectionParameters.ssl).toMatchObject({ rejectUnauthorized: true, servername: P.host });
});

test("wrong current_user, session_user, database or sessions schema owner refuses the owner connection", async () => {
  const good = { me: P.owner, session: P.owner, db: P.database, rolcanlogin: true, rolsuper: false, rolcreaterole: true,
    rolcreatedb: true, rolbypassrls: true, rolreplication: true, schema_owner: P.owner };
  const fake = (row: Record<string, unknown> | undefined) => ({ query: async () => ({ rows: row ? [row] : [] }) }) as any;
  expect(await code(() => requireReleaseOwner(fake(good)))).toBeNull();
  for (const drift of [{ me: "sessions_runtime" }, { session: "authenticator" }, { db: "other" }, { schema_owner: "supabase_admin" }, { rolsuper: true }])
    expect(await code(() => requireReleaseOwner(fake({ ...good, ...drift })))).toBe("release_owner");
  expect(await code(() => requireReleaseOwner(fake(undefined)))).toBe("release_owner");
});

test("workflow: the owner secret appears in exactly one step, the gate has no environment, proof has no secrets, plan 1d and receipt 30d", () => {
  const workflow = readFileSync(ADMISSION_RENEWAL_WORKFLOW, "utf8");
  expect(workflow.match(/secrets\.[A-Z_]+/gu)).toEqual(["secrets.SUPABASE_MIGRATION_OWNER_URL"]);
  expect(workflow).toContain("options: [readback, plan, renew]");
  expect(workflow).toContain("group: ${{ github.event_name == 'workflow_dispatch' && 'guarded-session-schema-production'");
  const proof = workflow.slice(workflow.indexOf("  proof:"), workflow.indexOf("  prerequisite-gate:"));
  expect(proof).toContain("if: github.event_name != 'workflow_dispatch'");
  expect(proof).not.toContain("secrets.");
  expect(proof).not.toContain("environment:");
  const gateJob = workflow.slice(workflow.indexOf("  prerequisite-gate:"), workflow.indexOf("  database:"));
  expect(gateJob).not.toContain("environment:\n");
  expect(gateJob).toContain("github.ref == 'refs/heads/main'");
  expect(gateJob).toContain(`github.repository_id == '${PRIVATE_INGEST_RELEASE.repositoryId}'`);
  expect(workflow).toContain("echo \"environment=session-schema-production\"");
  expect(workflow.indexOf("ingest-admission-renewal-github.ts --approved")).toBeLessThan(workflow.indexOf("secrets.SUPABASE_MIGRATION_OWNER_URL"));
  expect(workflow.match(/persist-credentials: false/gu)?.length).toBe(3);
  const plan = workflow.slice(workflow.indexOf("Publish the plan artifact"), workflow.indexOf("Publish the sanitized receipt"));
  expect(plan).toContain("retention-days: 1\n");
  expect(plan).toContain(`name: ${ADMISSION_RENEWAL_PLAN_ARTIFACT}\n`);
  expect(workflow.slice(workflow.indexOf("Publish the sanitized receipt"))).toContain("retention-days: 30\n");
  for (const forbidden of ["supabase login", "SET ROLE", "undo", "revert", P.project]) expect(workflow).not.toContain(forbidden);
});
