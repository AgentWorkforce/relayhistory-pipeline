import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { sessionIngestVars } from "../session-ingest.ts";
import {
  PRIVATE_INGEST_RELEASE as P,
  privateIngestActiveVersion,
  privateIngestProfileForPhase,
  privateIngestReleaseInputs,
  privateIngestReleasePhase,
  privateIngestTransitionReadback,
  privateIngestVersionReadback,
  type PrivateIngestReleasePhase,
  type CloudflareWorkerVersion,
} from "../../../src/session-ingest/private-ingest-release.ts";
import { privateIngestGithubGate } from "../../../scripts/private-ingest-release-github.ts";

const sha = "a".repeat(40), planRun = "90";
const kmsKeyArn = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000";

test("tracked production config is exact step 1; only the rollout phases and baseline rollback are representable", () => {
  const tracked = JSON.parse(readFileSync("config/cloudflare/private-ingest-production.json", "utf8"));
  const expected = privateIngestProfileForPhase("step-1-custody", kmsKeyArn);
  const { kmsKeyArn: _target, ...expectedTracked } = expected.sessionIngest;
  expect(tracked).toEqual({ sessionIngest: expectedTracked });
  for (const phase of ["baseline-drain", "step-0-shadow", "step-1-custody", "step-2-frozen-receipt", "step-4-async-on"] as const)
    expect(privateIngestReleasePhase(privateIngestProfileForPhase(phase, kmsKeyArn))).toBe(phase);
  for (const bad of [
    { ...tracked, sessionIngest: { ...tracked.sessionIngest, custodySource: "foreign" } },
    { ...tracked, sessionIngest: { ...tracked.sessionIngest, asyncAdmission: "off" } },
    { ...tracked, sessionIngest: { ...tracked.sessionIngest, claimTimeoutBusy: "on" } },
    { ...tracked, sessionIngest: { ...tracked.sessionIngest, expectedGeneration: "f".repeat(64) } },
  ]) expect(() => privateIngestReleasePhase(bad)).toThrow();
});

test("typed release confirmation binds mode, main SHA and prior plan", () => {
  expect(privateIngestReleaseInputs({
    PRIVATE_INGEST_RELEASE_MODE: "plan",
    PRIVATE_INGEST_RELEASE_SHA: sha,
  })).toMatchObject({ mode: "plan", sha, planRun: "" });
  const deploy = {
    PRIVATE_INGEST_RELEASE_MODE: "deploy",
    PRIVATE_INGEST_RELEASE_SHA: sha,
    PRIVATE_INGEST_RELEASE_PLAN_RUN: planRun,
    PRIVATE_INGEST_RELEASE_CONFIRMATION: `DEPLOY ${P.worker} ${sha} PLAN ${planRun}`,
  };
  expect(privateIngestReleaseInputs(deploy).mode).toBe("deploy");
  for (const [key, value] of Object.entries({
    PRIVATE_INGEST_RELEASE_SHA: "b".repeat(40),
    PRIVATE_INGEST_RELEASE_PLAN_RUN: "91",
    PRIVATE_INGEST_RELEASE_CONFIRMATION: "yes",
  })) expect(() => privateIngestReleaseInputs({ ...deploy, [key]: value })).toThrow();
});

function readbackFixture(phase: PrivateIngestReleasePhase) {
  const input = privateIngestProfileForPhase(phase, kmsKeyArn), vars = sessionIngestVars(input.sessionIngest);
  const bindings: any[] = Object.entries(vars).map(([name, text]) => ({ name, type: "plain_text", text }));
  bindings.push(
    { name: "HYPERDRIVE", type: "hyperdrive", id: input.sessionIngest.hyperdriveId },
    { name: "INGEST_FANOUT", type: "queue", queue_name: "96bulls-session-ingest-fanout" },
    { name: "INGEST_ADMISSION", type: "queue", queue_name: "96bulls-session-ingest-admission" },
    { name: "S3_ACCESS_KEY_ID", type: "secrets_store_secret", store_id: input.sessionIngest.secretsStoreId, secret_name: "96bulls-session-ingest-s3-access-key-id" },
    { name: "S3_SECRET_ACCESS_KEY", type: "secrets_store_secret", store_id: input.sessionIngest.secretsStoreId, secret_name: "96bulls-session-ingest-s3-secret-access-key" },
    { name: "INGEST_SIGNING_KEY", type: "secrets_store_secret", store_id: input.sessionIngest.secretsStoreId, secret_name: "96bulls-session-ingest-signing-key" },
    { name: "worker-module", type: "wasm_module" },
  );
  const active = { deploymentId: "1".repeat(8) + "-1111-1111-1111-111111111111", versionId: "2".repeat(8) + "-2222-2222-2222-222222222222", createdOn: "2026-11-11T00:00:00Z" };
  const version: CloudflareWorkerVersion = {
    id: active.versionId,
    resources: {
      bindings,
      script_runtime: {
        compatibility_date: "2026-10-01",
        compatibility_flags: ["nodejs_compat", "new_module_registry"],
      },
    },
  };
  return { input, active, version };
}

test("post-deploy readback requires one 100% version and every targeted binding", () => {
  const selected = privateIngestActiveVersion({ deployments: [{
    id: "1".repeat(8) + "-1111-1111-1111-111111111111",
    created_on: "2026-11-11T00:00:00Z",
    versions: [{ version_id: "2".repeat(8) + "-2222-2222-2222-222222222222", percentage: 100 }],
  }, {
    id: "9".repeat(8) + "-9999-9999-9999-999999999999",
    versions: [{ version_id: "8".repeat(8) + "-8888-8888-8888-888888888888", percentage: 100 }],
  }] });
  const fixture = readbackFixture("step-2-frozen-receipt");
  expect(privateIngestVersionReadback(fixture.input, selected, fixture.version)).toMatchObject({
    status: "PRIVATE_INGEST_LIVE_READBACK_PASS",
    phase: "step-2-frozen-receipt",
  });
  expect(() => privateIngestActiveVersion({ deployments: [] })).toThrow();
  expect(() => privateIngestActiveVersion({ deployments: [{ id: selected.deploymentId, versions: [
    { version_id: selected.versionId, percentage: 50 }, { version_id: "3".repeat(8) + "-3333-3333-3333-333333333333", percentage: 50 },
  ] }] })).toThrow();
  for (const mutate of [
    (v: CloudflareWorkerVersion) => { v.resources!.bindings!.find((b) => b.name === "INGEST_CUSTODY_SOURCE")!.text = "shadow"; },
    (v: CloudflareWorkerVersion) => { v.resources!.bindings!.push({ name: "INGEST_FOREIGN", type: "plain_text", text: "on" }); },
    (v: CloudflareWorkerVersion) => { v.resources!.bindings = v.resources!.bindings!.filter((b) => b.name !== "INGEST_ADMISSION"); },
    (v: CloudflareWorkerVersion) => { v.resources!.bindings!.find((b) => b.name === "HYPERDRIVE")!.id = "f".repeat(32); },
    (v: CloudflareWorkerVersion) => { delete v.resources!.script_runtime; },
    (v: CloudflareWorkerVersion) => { v.resources!.script_runtime!.compatibility_date = "2026-09-01"; },
    (v: CloudflareWorkerVersion) => { v.resources!.script_runtime!.compatibility_flags = []; },
  ]) {
    const copy = structuredClone(fixture.version);
    mutate(copy);
    expect(() => privateIngestVersionReadback(fixture.input, selected, copy)).toThrow();
  }
});

test("shadow is drain-only and cannot smuggle the receipt bridge or async activation", () => {
  const shadow = privateIngestProfileForPhase("step-0-shadow", kmsKeyArn);
  expect(shadow.sessionIngest.custodySource).toBe("shadow");
  expect(shadow.sessionIngest.asyncAdmission).toBe("drain");
  expect(shadow.sessionIngest.syncAdmissionFrozenReceipt).toBeUndefined();
  for (const custodySource of [undefined, "shadow"] as const) {
    for (const overrides of [
      { asyncAdmission: "on" },
      { syncAdmissionFrozenReceipt: "on" },
      { asyncAdmission: "on", syncAdmissionFrozenReceipt: "on" },
    ]) expect(() => privateIngestReleasePhase({
      sessionIngest: { ...shadow.sessionIngest, custodySource, ...overrides },
    } as any)).toThrow("private_ingest_profile_phase");
  }
});

test("CI shadow transition and readback use the live phase without skipping advanced rollout steps", () => {
  for (const [from, to] of [
    ["baseline-drain", "step-0-shadow"],
    ["step-0-shadow", "step-0-shadow"],
    ["step-0-shadow", "baseline-drain"],
    ["step-0-shadow", "step-1-custody"],
    ["step-1-custody", "step-0-shadow"],
  ] as const) {
    const f = readbackFixture(from);
    expect(privateIngestTransitionReadback(privateIngestProfileForPhase(to, kmsKeyArn), f.active, f.version))
      .toMatchObject({ from, to });
    expect(privateIngestVersionReadback(f.input, f.active, f.version).phase).toBe(from);
  }
  for (const [from, to] of [
    ["step-0-shadow", "step-2-frozen-receipt"],
    ["step-0-shadow", "step-4-async-on"],
    ["step-2-frozen-receipt", "step-0-shadow"],
    ["step-4-async-on", "step-0-shadow"],
  ] as const) {
    const f = readbackFixture(from);
    expect(() => privateIngestTransitionReadback(privateIngestProfileForPhase(to, kmsKeyArn), f.active, f.version))
      .toThrow("private_ingest_live_transition");
  }
  const f = readbackFixture("step-0-shadow");
  f.version.resources!.bindings!.find((b) => b.name === "INGEST_CUSTODY_SOURCE")!.text = "frozen-supabase";
  expect(() => privateIngestVersionReadback(f.input, f.active, f.version)).toThrow();
});

test("live transition gate requires an independently deployed step 2 before step 4", () => {
  const baseline = readbackFixture("step-1-custody");
  baseline.version.resources!.bindings = baseline.version.resources!.bindings!.filter(
    (binding) => binding.name !== "INGEST_CUSTODY_SOURCE",
  );
  expect(privateIngestTransitionReadback(
    privateIngestProfileForPhase("step-1-custody", kmsKeyArn),
    baseline.active,
    baseline.version,
  )).toMatchObject({ from: "baseline-drain", to: "step-1-custody" });
  expect(privateIngestTransitionReadback(
    privateIngestProfileForPhase("baseline-drain", kmsKeyArn),
    baseline.active,
    baseline.version,
  )).toMatchObject({ from: "baseline-drain", to: "baseline-drain" });
  const currentStep1ForRollback = readbackFixture("step-1-custody");
  expect(privateIngestTransitionReadback(
    privateIngestProfileForPhase("baseline-drain", kmsKeyArn),
    currentStep1ForRollback.active,
    currentStep1ForRollback.version,
  )).toMatchObject({ from: "step-1-custody", to: "baseline-drain" });
  const currentStep1 = readbackFixture("step-1-custody");
  expect(privateIngestTransitionReadback(
    privateIngestProfileForPhase("step-2-frozen-receipt", kmsKeyArn),
    currentStep1.active,
    currentStep1.version,
  )).toMatchObject({ from: "step-1-custody", to: "step-2-frozen-receipt" });
  expect(() => privateIngestTransitionReadback(
    privateIngestProfileForPhase("step-4-async-on", kmsKeyArn),
    currentStep1.active,
    currentStep1.version,
  )).toThrow("private_ingest_live_transition");
  const currentStep2 = readbackFixture("step-2-frozen-receipt");
  expect(privateIngestTransitionReadback(
    privateIngestProfileForPhase("step-4-async-on", kmsKeyArn),
    currentStep2.active,
    currentStep2.version,
  )).toMatchObject({ from: "step-2-frozen-receipt", to: "step-4-async-on" });
});

function githubFixture() {
  const actor = { id: P.releaseActorGithubUserId, type: "User" };
  const run = { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: P.workflow,
    run_attempt: 1, actor, triggering_actor: actor, created_at: new Date().toISOString() };
  return {
    ref: { object: { sha } }, run,
    environment: {
      id: 7, name: P.environment,
      protection_rules: [{ type: "branch_policy" }],
      deployment_branch_policy: { protected_branches: false, custom_branch_policies: true },
    },
    branches: { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] },
    ci: { workflow_runs: [{ id: 10, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] },
    jobs: { total_count: 2, jobs: ["unit-tests", "session-read-worker / worker"].map((name) =>
      ({ name, head_sha: sha, status: "completed", conclusion: "success" })) },
    plan: { ...run, id: Number(planRun), status: "completed", conclusion: "success", created_at: new Date().toISOString() },
    artifacts: { total_count: 1, artifacts: [{ name: "private-ingest-plan", expired: false }] },
  };
}

function githubGet(f: ReturnType<typeof githubFixture>) {
  return async (path: string): Promise<any> => {
    if (path.endsWith("/git/ref/heads/main")) return f.ref;
    if (path.endsWith("/actions/runs/100")) return f.run;
    if (path.endsWith(`/actions/runs/${planRun}`)) return f.plan;
    if (path.endsWith("/environments/" + P.environment)) return f.environment;
    if (path.includes("deployment-branch-policies")) return f.branches;
    if (path.includes("/workflows/ci.yml/")) return f.ci;
    if (path.includes("/runs/10/")) return f.jobs;
    if (path.endsWith(`/actions/runs/${planRun}/artifacts?per_page=100`)) return f.artifacts;
    throw new Error("unexpected " + path);
  };
}

const githubBase = {
  PRIVATE_INGEST_RELEASE_MODE: "deploy",
  PRIVATE_INGEST_RELEASE_SHA: sha,
  PRIVATE_INGEST_RELEASE_PLAN_RUN: planRun,
  PRIVATE_INGEST_RELEASE_CONFIRMATION: `DEPLOY ${P.worker} ${sha} PLAN ${planRun}`,
  GITHUB_REPOSITORY: P.repository,
  GITHUB_REPOSITORY_ID: P.repositoryId,
  GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main",
  GITHUB_SHA: sha,
  GITHUB_RUN_ATTEMPT: "1",
  GITHUB_RUN_ID: "100",
};

test("GitHub gate requires exact main, green CI, exact Khaliq dispatch identity, environment and prior plan", async () => {
  expect((await privateIngestGithubGate(githubBase, githubGet(githubFixture()), true)).mode).toBe("deploy");
  const changes: Array<(f: ReturnType<typeof githubFixture>) => void> = [
    (f) => { f.ref.object.sha = "b".repeat(40); },
    (f) => { f.ci.workflow_runs[0]!.conclusion = "failure"; },
    (f) => { f.environment.protection_rules.push({ type: "required_reviewers" }); },
    (f) => { f.run.actor.id = 42; },
    (f) => { f.run.triggering_actor.id = 42; },
    (f) => { f.artifacts.artifacts = []; f.artifacts.total_count = 0; },
    (f) => { f.plan.head_sha = "b".repeat(40); },
  ];
  for (const mutate of changes) {
    const fixture = githubFixture();
    mutate(fixture);
    await expect(privateIngestGithubGate(githubBase, githubGet(fixture), true)).rejects.toThrow();
  }
});

test("workflow is CI-only and binds production credentials only after install", () => {
  const workflow = readFileSync(".github/workflows/deploy-private-ingest-worker.yml", "utf8");
  expect(workflow).toContain("environment:");
  expect(workflow).toContain("private-ingest-production");
  expect(workflow).toContain("PRIVATE_INGEST_GITHUB_GATE: approved");
  expect(workflow.indexOf("Install without production credentials")).toBeLessThan(workflow.indexOf("CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}"));
  expect(workflow).not.toContain("wrangler deploy");
  expect(workflow).not.toContain("ssh ");
  expect(readFileSync("scripts/private-ingest-ci.ts", "utf8")).toContain(
    'resolve(".96bulls/private-ingest-release/preflight.json")',
  );
});
