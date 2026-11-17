/**
 * GitHub gate and workflow shape for the dormant org admission repair (scripts/ingest-org-admission-repair-github.ts,
 * .github/workflows/ingest-org-admission-repair.yml). The gate reuses the admission renewal gate's helpers; the owner
 * connection cases are in ingest-admission-renewal-gate.test.ts (same releaseClient / requireReleaseOwner). The
 * production project ref is only ever referenced through SCHEMA_RELEASE; it is never written here.
 */
import { expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { orgAdmissionRepairGithubGate } from "../../../scripts/ingest-org-admission-repair-github.ts";
import { orgAdmissionRepairEnv, orgAdmissionRepairMasks } from "../ingest-org-admission-repair-operator.ts";
import { SCHEMA_RELEASE as P } from "../schema-release-contract.ts";
import { PRIVATE_INGEST_RELEASE } from "../../session-ingest/private-ingest-release.ts";
import {
  ORG_ADMISSION_REPAIR_ENABLED,
  ORG_ADMISSION_REPAIR_PLAN_ARTIFACT,
  ORG_ADMISSION_REPAIR_WORKFLOW,
} from "../org-admission-repair-contract.ts";

const sha = "1".repeat(40), planRun = "424242", G = "a".repeat(64), O = "c".repeat(64);
const code = async (fn: () => unknown) => {
  try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};

function githubFixture() {
  const actor = { id: P.dispatcher.id, login: P.dispatcher.login, type: "User" };
  const run = { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: ORG_ADMISSION_REPAIR_WORKFLOW,
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
    artifacts: { total_count: 1, artifacts: [{ name: ORG_ADMISSION_REPAIR_PLAN_ARTIFACT, expired: false }] as any[] },
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
const phrase = `ADMIT-ORGS ${P.project} ${sha} GENERATION ${G} VALID_UNTIL 2026-12-01T00:00:00.000Z ORGS ${O} COUNT 2 PLAN ${planRun}`;
const env = (mode: string, extra: Record<string, string> = {}) =>
  ({ ...context, ORG_REPAIR_MODE: mode, ORG_REPAIR_SHA: sha, ORG_REPAIR_ORGS: "org-a,org-b", ...extra });
const admitEnv = env("admit", { ORG_REPAIR_PLAN_RUN: planRun, ORG_REPAIR_CONFIRMATION: phrase });
const gate = (e: Record<string, string>, f: Fixture, enabled = true) =>
  code(() => orgAdmissionRepairGithubGate(e, githubGet(f), true, { enabled }));

test("dormant gate: plan and admit refuse while disabled; readback passes every gate", async () => {
  expect(ORG_ADMISSION_REPAIR_ENABLED).toBe(false);
  expect(await gate(env("plan"), githubFixture(), false)).toBe("org_admission_repair_dormant");
  expect(await gate(admitEnv, githubFixture(), false)).toBe("org_admission_repair_dormant");
  expect(await gate(env("readback"), githubFixture(), false)).toBeNull();
  expect(await gate(env("plan"), githubFixture())).toBeNull();
  expect(await gate(admitEnv, githubFixture())).toBeNull();
});

test("stale main, context, dispatcher, plan run, artifact and approval refuse the gate", async () => {
  const failures: Array<[Record<string, string>, (f: Fixture) => void, string]> = [
    [admitEnv, (f) => { f.ref.object.sha = "b".repeat(40); }, "org_admission_repair_gate_main_moved"],
    [{ ...admitEnv, GITHUB_SHA: "b".repeat(40) }, () => {}, "org_admission_repair_gate_context"],
    [{ ...admitEnv, GITHUB_RUN_ATTEMPT: "2" }, () => {}, "org_admission_repair_gate_context"],
    [{ ...admitEnv, GITHUB_REF: "refs/heads/dev" }, () => {}, "org_admission_repair_gate_context"],
    [{ ...admitEnv, GITHUB_REPOSITORY_ID: "1" }, () => {}, "org_admission_repair_gate_context"],
    [admitEnv, (f) => { f.run.actor.id = 42; }, "org_admission_repair_gate_run"],
    [admitEnv, (f) => { f.run.triggering_actor.login = "someone"; }, "org_admission_repair_gate_run"],
    [admitEnv, (f) => { f.run.path = ".github/workflows/ingest-admission-renewal.yml"; }, "org_admission_repair_gate_run"],
    [admitEnv, (f) => { f.environment.deployment_branch_policy.protected_branches = true; }, "org_admission_repair_gate_environment"],
    [admitEnv, (f) => { f.runs.workflow_runs[0]!.conclusion = "failure"; }, "org_admission_repair_gate_ci"],
    [admitEnv, (f) => { f.plan.path = ".github/workflows/ingest-admission-renewal.yml"; }, "org_admission_repair_gate_plan_run"],
    [admitEnv, (f) => { f.plan.actor.id = 42; }, "org_admission_repair_gate_plan_run"],
    [admitEnv, (f) => { f.plan.conclusion = "failure"; }, "org_admission_repair_gate_plan_run"],
    [admitEnv, (f) => { f.plan.created_at = new Date(Date.now() - 24 * 3600_000 - 1000).toISOString(); }, "org_admission_repair_gate_plan_run"],
    [admitEnv, (f) => { f.artifacts.artifacts = []; }, "org_admission_repair_gate_plan_artifact"],
    [admitEnv, (f) => { f.artifacts.artifacts[0]!.expired = true; }, "org_admission_repair_gate_plan_artifact"],
    [admitEnv, (f) => { f.environment.protection_rules.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] }); },
      "org_admission_repair_gate_human_approval"],
  ];
  for (const [input, mutate, expected] of failures) {
    const f = githubFixture();
    mutate(f);
    expect(await gate(input, f)).toBe(expected);
  }
});

test("confirmation shape and org list are checked before any GitHub read", async () => {
  for (const bad of [phrase.replace(P.project, "fixtureprojectref000"), phrase.replace("COUNT 2", "COUNT 3"),
    phrase.replace(`PLAN ${planRun}`, "PLAN 1"), phrase + " ", "admit-orgs " + phrase.slice(11)])
    expect(await gate({ ...admitEnv, ORG_REPAIR_CONFIRMATION: bad }, githubFixture())).toBe("org_admission_repair_confirmation");
  expect(await gate(env("readback", { ORG_REPAIR_CONFIRMATION: phrase }), githubFixture())).toBe("org_admission_repair_confirmation");
  expect(await gate(env("readback", { ORG_REPAIR_ORGS: "org a" }), githubFixture())).toBe("org_admission_repair_orgs");
  expect(await gate(env("readback", { ORG_REPAIR_ORGS: "" }), githubFixture())).toBe("org_admission_repair_orgs");
  expect(await gate(env("undo"), githubFixture())).toBe("org_admission_repair_inputs");
});

test("event payload: orgs are read from GITHUB_EVENT_PATH and masked; an unreadable payload refuses", async () => {
  const dir = mkdtempSync(join(tmpdir(), "oar-event-"));
  try {
    const path = join(dir, "event.json");
    writeFileSync(path, JSON.stringify({ inputs: { orgs: "org-b,org-a" } }));
    const e = orgAdmissionRepairEnv({ ORG_REPAIR_MODE: "readback", GITHUB_EVENT_PATH: path });
    expect(e.ORG_REPAIR_ORGS).toBe("org-b,org-a");
    expect(orgAdmissionRepairEnv({ ORG_REPAIR_ORGS: "x", GITHUB_EVENT_PATH: path }).ORG_REPAIR_ORGS).toBe("x");
    expect(orgAdmissionRepairMasks(["org-a", "org-b"])).toBe("::add-mask::org-a\n::add-mask::org-b");
    expect(await code(() => orgAdmissionRepairEnv({ GITHUB_EVENT_PATH: join(dir, "missing.json") }))).toBe("org_admission_repair_orgs");
    writeFileSync(path, JSON.stringify({ inputs: {} }));
    expect(await gate({ ...env("readback"), ORG_REPAIR_ORGS: undefined as never, ...orgAdmissionRepairEnv({ GITHUB_EVENT_PATH: path }) } as never, githubFixture()))
      .toBe("org_admission_repair_orgs");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("workflow: the owner secret appears in exactly one step, orgs never interpolated, no environment on the gate, plan 1d and receipt 30d", () => {
  const workflow = readFileSync(ORG_ADMISSION_REPAIR_WORKFLOW, "utf8");
  expect(workflow.match(/secrets\.[A-Z_]+/gu)).toEqual(["secrets.SUPABASE_MIGRATION_OWNER_URL"]);
  expect(workflow).toContain("options: [readback, plan, admit]");
  expect(workflow).toContain("group: ${{ github.event_name == 'workflow_dispatch' && 'guarded-session-schema-production'");
  // The org list is never interpolated (run name, env or shell): the scripts read it from the event payload.
  expect(workflow).not.toContain("inputs.orgs");
  expect(workflow).not.toContain("ORG_REPAIR_ORGS");
  expect(workflow).not.toContain("github.event.inputs");
  const proof = workflow.slice(workflow.indexOf("  proof:"), workflow.indexOf("  prerequisite-gate:"));
  expect(proof).toContain("if: github.event_name != 'workflow_dispatch'");
  expect(proof).not.toContain("secrets.");
  expect(proof).not.toContain("environment:");
  const gateJob = workflow.slice(workflow.indexOf("  prerequisite-gate:"), workflow.indexOf("  database:"));
  expect(gateJob).not.toContain("environment:\n");
  expect(gateJob).toContain("github.ref == 'refs/heads/main'");
  expect(gateJob).toContain(`github.repository_id == '${PRIVATE_INGEST_RELEASE.repositoryId}'`);
  expect(workflow).toContain("echo \"environment=session-schema-production\"");
  expect(workflow.indexOf("ingest-org-admission-repair-github.ts --approved")).toBeLessThan(workflow.indexOf("secrets.SUPABASE_MIGRATION_OWNER_URL"));
  expect(workflow.match(/persist-credentials: false/gu)?.length).toBe(3);
  const plan = workflow.slice(workflow.indexOf("Publish the plan artifact"), workflow.indexOf("Publish the sanitized receipt"));
  expect(plan).toContain("retention-days: 1\n");
  expect(plan).toContain(`name: ${ORG_ADMISSION_REPAIR_PLAN_ARTIFACT}\n`);
  expect(workflow.slice(workflow.indexOf("Publish the sanitized receipt"))).toContain("retention-days: 30\n");
  for (const forbidden of ["supabase login", "SET ROLE", "undo", "revert", P.project]) expect(workflow).not.toContain(forbidden);
});
