/**
 * Generation overlap on the private-ingest release contract: dormant by default (byte-identical to the
 * no-overlap contract), and with a reviewed pair exactly G -> G+H -> H plus the rollback G+H -> G.
 */
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { sessionIngestVars } from "../session-ingest.ts";
import { inputsDigest } from "../../aws/deploy-guard.ts";
import {
  PRIVATE_INGEST_GENERATION_CONTRACT,
  PRIVATE_INGEST_OVERLAP,
  PRIVATE_INGEST_RELEASE as P,
  privateIngestProfileForPhase,
  privateIngestReleaseInputs,
  privateIngestReleasePhase,
  privateIngestTransitionReadback,
  privateIngestVersionReadback,
  type CloudflareWorkerVersion,
  type PrivateIngestGenerationContract,
} from "../../../src/session-ingest/private-ingest-release.ts";
import { privateIngestGithubGate, trackedOverlapGeneration } from "../../../scripts/private-ingest-release-github.ts";
import { overlapGithubGate, OVERLAP_WORKFLOW } from "../../../scripts/ingest-generation-overlap-github.ts";
import { acceptGenerationsDeletionAllowed } from "../../../scripts/private-ingest-operator.ts";
import { SCHEMA_RELEASE } from "../../../src/hoopdb/schema-release-contract.ts";
import type { CloudflareStageInputs } from "../alchemy.cloudflare.run.ts";

const kmsKeyArn = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000";
const sha = "a".repeat(40), planRun = "90";
const tracked = JSON.parse(readFileSync("config/cloudflare/private-ingest-production.json", "utf8"));
const G: string = tracked.sessionIngest.expectedGeneration;
const H = G === "e".repeat(64) ? "d".repeat(64) : "e".repeat(64);
const X = G === "c".repeat(64) ? "b".repeat(64) : "c".repeat(64);
const atG: PrivateIngestGenerationContract = { generation: G, overlap: { from: G, to: H } };
const atH: PrivateIngestGenerationContract = { generation: H, overlap: { from: G, to: H } };
const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");
const code = (fn: () => unknown) => {
  try { fn(); return null; } catch (error) { return (error as { code?: string }).code ?? "?"; }
};
const withTarget = (sessionIngest: Record<string, unknown>): CloudflareStageInputs =>
  ({ ...tracked, sessionIngest: { ...sessionIngest, kmsKeyArn } }) as CloudflareStageInputs;

test("dormant by default: the tracked production profile plans, confirms and reads back byte-identically", () => {
  expect(PRIVATE_INGEST_OVERLAP).toBeNull();
  expect(PRIVATE_INGEST_GENERATION_CONTRACT).toEqual({ generation: G, overlap: null });
  // Digests captured at the base commit (7c3a8295) before this change, with the test KMS target.
  expect(sha256(readFileSync("config/cloudflare/private-ingest-production.json", "utf8")))
    .toBe("3c8a5c3b5ef52c4b1ee0246fd931ee4286592ddefa99d3ec96bae25b1350509c");
  const input = withTarget(tracked.sessionIngest);
  expect(inputsDigest(JSON.stringify(input, null, 2) + "\n"))
    .toBe("3e6ba55f3110efa6729f4780ac689c9204aea8716e428640758c0fe1126d1fe4");
  expect(sha256(JSON.stringify(sessionIngestVars(input.sessionIngest))))
    .toBe("7bdde12da2f9476ec63994cc0419a423a7f9187764090b031d67f6ab33c4f575");
  expect(sha256(JSON.stringify(input.sessionIngest)))
    .toBe("41eaf5aac40a7b63f13deea9e5976cd233faba18d96886b25506ab067200e7c3");
  expect(sessionIngestVars(input.sessionIngest).INGEST_ACCEPT_GENERATIONS).toBeUndefined();
  expect(privateIngestReleasePhase(input)).toBe("step-1-custody");
  expect(trackedOverlapGeneration()).toBeUndefined();
  expect(privateIngestReleaseInputs({
    PRIVATE_INGEST_RELEASE_MODE: "deploy",
    PRIVATE_INGEST_RELEASE_SHA: sha,
    PRIVATE_INGEST_RELEASE_PLAN_RUN: planRun,
    PRIVATE_INGEST_RELEASE_CONFIRMATION: `DEPLOY ${P.worker} ${sha} PLAN ${planRun}`,
  }, trackedOverlapGeneration()).mode).toBe("deploy");
  // A dormant contract refuses any tracked overlap and cannot build an overlap phase.
  expect(code(() => privateIngestReleasePhase(withTarget({ ...tracked.sessionIngest, overlapGeneration: H }))))
    .toBe("private_ingest_profile_overlap_unreviewed");
  expect(code(() => privateIngestProfileForPhase("step-1-custody-overlap", kmsKeyArn)))
    .toBe("private_ingest_profile_overlap_unreviewed");
  expect(code(() => privateIngestProfileForPhase("step-0-shadow-overlap", kmsKeyArn)))
    .toBe("private_ingest_profile_overlap_unreviewed");
  expect(code(() => privateIngestReleasePhase(withTarget({
    ...tracked.sessionIngest, custodySource: "shadow", overlapGeneration: H,
  })))).toBe("private_ingest_profile_overlap_unreviewed");
});

test("overlapGeneration: malformed, equal, non-buffered, unreviewed, stale and unexpected values are refused distinctly", () => {
  const profile = (overlapGeneration: unknown, extra: Record<string, unknown> = {}) =>
    withTarget({ ...tracked.sessionIngest, ...extra, overlapGeneration });
  expect(privateIngestReleasePhase(profile(H), atG)).toBe("step-1-custody-overlap");
  for (const bad of ["E".repeat(64), H.slice(1), H + "0", "", 7, null, [H]])
    expect(code(() => privateIngestReleasePhase(profile(bad), atG))).toBe("private_ingest_profile_overlap_malformed");
  expect(code(() => privateIngestReleasePhase(profile(G), atG))).toBe("private_ingest_profile_overlap_equal");
  expect(code(() => privateIngestReleasePhase(profile(H, { acceptanceMode: "canonical" }), atG)))
    .toBe("private_ingest_profile_overlap_mode");
  expect(code(() => privateIngestReleasePhase(profile(H), { generation: G, overlap: null })))
    .toBe("private_ingest_profile_overlap_unreviewed");
  // Stale: the reviewed contract already moved to H, so offering {H, G} is a downgrade attempt.
  expect(code(() => privateIngestReleasePhase(profile(G, { expectedGeneration: H }), atH)))
    .toBe("private_ingest_profile_overlap_stale");
  expect(code(() => privateIngestReleasePhase(profile(X), atG))).toBe("private_ingest_profile_overlap_unexpected");
  // The expected generation itself is still the reviewed one.
  expect(code(() => privateIngestReleasePhase(profile(H, { expectedGeneration: X }), atG)))
    .toBe("private_ingest_profile_common");
  // A malformed reviewed pair is refused before any profile is trusted.
  for (const bad of [
    { generation: G, overlap: { from: G, to: G } },
    { generation: X, overlap: { from: G, to: H } },
    { generation: G, overlap: { from: G, to: "F".repeat(64) } },
    { generation: "g", overlap: null },
  ]) expect(code(() => privateIngestReleasePhase(withTarget(tracked.sessionIngest), bad)))
    .toBe("private_ingest_overlap_contract");
  // The base phases stay exactly as before under a reviewed pair.
  for (const phase of ["baseline-drain", "step-0-shadow", "step-1-custody", "step-2-frozen-receipt", "step-4-async-on"] as const) {
    expect(privateIngestReleasePhase(privateIngestProfileForPhase(phase, kmsKeyArn, atG), atG)).toBe(phase);
    expect(privateIngestReleasePhase(privateIngestProfileForPhase(`${phase}-overlap`, kmsKeyArn, atG), atG))
      .toBe(`${phase}-overlap`);
    expect(privateIngestReleasePhase(privateIngestProfileForPhase(phase, kmsKeyArn, atH), atH)).toBe(phase);
    expect(code(() => privateIngestProfileForPhase(`${phase}-overlap`, kmsKeyArn, atH)))
      .toBe("private_ingest_profile_overlap_stale");
  }
  const overlapProfile = privateIngestProfileForPhase("step-1-custody-overlap", kmsKeyArn, atG).sessionIngest;
  expect(sessionIngestVars(overlapProfile).INGEST_ACCEPT_GENERATIONS).toBe(`${G},${H}`);
});

test("the deploy confirmation grows OVERLAP <H> only when the tracked profile declares an overlap", async () => {
  const deploy = {
    PRIVATE_INGEST_RELEASE_MODE: "deploy",
    PRIVATE_INGEST_RELEASE_SHA: sha,
    PRIVATE_INGEST_RELEASE_PLAN_RUN: planRun,
    PRIVATE_INGEST_RELEASE_CONFIRMATION: `DEPLOY ${P.worker} ${sha} PLAN ${planRun} OVERLAP ${H}`,
  };
  expect(privateIngestReleaseInputs(deploy, H).confirmation).toBe(deploy.PRIVATE_INGEST_RELEASE_CONFIRMATION);
  expect(code(() => privateIngestReleaseInputs(deploy))).toBe("private_ingest_release_confirmation");
  expect(code(() => privateIngestReleaseInputs(deploy, X))).toBe("private_ingest_release_confirmation");
  expect(code(() => privateIngestReleaseInputs({ ...deploy,
    PRIVATE_INGEST_RELEASE_CONFIRMATION: `DEPLOY ${P.worker} ${sha} PLAN ${planRun}` }, H)))
    .toBe("private_ingest_release_confirmation");
  for (const bad of ["E".repeat(64), "", 1, null])
    expect(code(() => privateIngestReleaseInputs(deploy, bad))).toBe("private_ingest_release_overlap");
  expect(privateIngestReleaseInputs({ PRIVATE_INGEST_RELEASE_MODE: "plan", PRIVATE_INGEST_RELEASE_SHA: sha }, H).mode)
    .toBe("plan");
});

function liveFixture(input: CloudflareStageInputs) {
  const vars = sessionIngestVars(input.sessionIngest);
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
    resources: { bindings, script_runtime: { compatibility_date: "2026-10-01", compatibility_flags: ["nodejs_compat", "new_module_registry"] } },
  };
  return { active, version };
}

const liveSingleG = (base = "step-1-custody" as const) => liveFixture(privateIngestProfileForPhase(base, kmsKeyArn, atG));
const liveSingleH = (base = "step-1-custody" as const) => liveFixture(privateIngestProfileForPhase(base, kmsKeyArn, atH));
const liveOverlap = (base = "step-1-custody" as const) =>
  liveFixture(privateIngestProfileForPhase(`${base}-overlap`, kmsKeyArn, atG));

test("live readback admits INGEST_ACCEPT_GENERATIONS only when declared; refuses undeclared, mismatched and unknown bindings", () => {
  const overlapInput = privateIngestProfileForPhase("step-1-custody-overlap", kmsKeyArn, atG);
  const live = liveOverlap();
  // The version readback uses the production contract, which is dormant here: an overlap profile refuses.
  expect(() => privateIngestVersionReadback(overlapInput, live.active, live.version)).toThrow("private_ingest_profile_overlap_unreviewed");
  // Causal revert: the profile (and contract) dropped the overlap while the live Worker still accepts {G, H}.
  const single = privateIngestProfileForPhase("step-1-custody", kmsKeyArn);
  expect(code(() => privateIngestVersionReadback(single, live.active, live.version))).toBe("private_ingest_live_undeclared_overlap");
  // The same under the transition gate, with a dormant contract: its own code, never an ambiguous miss.
  expect(code(() => privateIngestTransitionReadback(single, live.active, live.version)))
    .toBe("private_ingest_live_undeclared_overlap");
  // Declared overlap but the live accept set names another generation, or is missing.
  const wrong = structuredClone(live.version);
  wrong.resources!.bindings!.find((b) => b.name === "INGEST_ACCEPT_GENERATIONS")!.text = `${G},${X}`;
  expect(code(() => privateIngestTransitionReadback(overlapInput, live.active, wrong, atG))).toBe("private_ingest_live_phase");
  const missing = liveSingleG();
  expect(code(() => privateIngestTransitionReadback(privateIngestProfileForPhase("step-2-frozen-receipt-overlap", kmsKeyArn, atG), missing.active, missing.version, atG)))
    .toBe("private_ingest_live_transition");
  // An unknown live INGEST_* binding never matches any phase.
  const unknown = structuredClone(live.version);
  unknown.resources!.bindings!.push({ name: "INGEST_ACCEPT_GENERATIONS_EXTRA", type: "plain_text", text: H });
  expect(code(() => privateIngestTransitionReadback(overlapInput, live.active, unknown, atG))).toBe("private_ingest_live_phase");
  const unknownSingle = structuredClone(liveSingleG().version);
  unknownSingle.resources!.bindings!.push({ name: "INGEST_FOREIGN", type: "plain_text", text: "on" });
  expect(code(() => privateIngestVersionReadback(single, liveSingleG().active, unknownSingle))).toBe("private_ingest_live_unknown_flag");
});

test("G -> G+H -> H and the rollback G+H -> G are the only generation moves, with the rollout phase held", () => {
  const to = (phase: Parameters<typeof privateIngestProfileForPhase>[0], contract: PrivateIngestGenerationContract) =>
    privateIngestProfileForPhase(phase, kmsKeyArn, contract);
  const pass = (input: CloudflareStageInputs, live: ReturnType<typeof liveFixture>, contract: PrivateIngestGenerationContract) =>
    privateIngestTransitionReadback(input, live.active, live.version, contract);
  const g = liveSingleG(), both = liveOverlap(), h = liveSingleH();
  expect(pass(to("step-1-custody-overlap", atG), g, atG))
    .toMatchObject({ from: "step-1-custody", to: "step-1-custody-overlap", generationFrom: "G", generationTo: "G+H" });
  expect(pass(to("step-1-custody", atH), both, atH))
    .toMatchObject({ from: "step-1-custody-overlap", to: "step-1-custody", generationFrom: "G+H", generationTo: "H" });
  expect(pass(to("step-1-custody", atG), both, atG))
    .toMatchObject({ from: "step-1-custody-overlap", to: "step-1-custody", generationFrom: "G+H", generationTo: "G" });
  // Same-state redeploys are no-moves.
  expect(pass(to("step-1-custody-overlap", atG), both, atG)).toMatchObject({ generationFrom: "G+H", generationTo: "G+H" });
  expect(pass(to("step-1-custody", atH), h, atH)).toMatchObject({ generationFrom: "H", generationTo: "H" });
  expect(pass(to("step-1-custody", atG), g, atG)).toMatchObject({ generationFrom: "G", generationTo: "G" });
  // The existing phase table still applies within one generation state.
  expect(pass(to("step-2-frozen-receipt", atH), h, atH)).toMatchObject({ from: "step-1-custody", to: "step-2-frozen-receipt" });
  // A dormant receipt keeps its exact shape.
  const dormant = liveFixture(privateIngestProfileForPhase("step-1-custody", kmsKeyArn));
  expect(Object.keys(privateIngestTransitionReadback(privateIngestProfileForPhase("step-1-custody", kmsKeyArn), dormant.active, dormant.version)))
    .toEqual(["status", "from", "to", "deploymentId", "versionId"]);
});

test("refused generation transitions: H -> G+H, G -> H, H -> G, combined phase moves and phase moves while overlapped", () => {
  const refused = (input: CloudflareStageInputs, live: ReturnType<typeof liveFixture>, contract: PrivateIngestGenerationContract) =>
    code(() => privateIngestTransitionReadback(input, live.active, live.version, contract));
  const to = (phase: Parameters<typeof privateIngestProfileForPhase>[0], contract: PrivateIngestGenerationContract) =>
    privateIngestProfileForPhase(phase, kmsKeyArn, contract);
  expect(refused(to("step-1-custody-overlap", atG), liveSingleH(), atG)).toBe("private_ingest_live_transition"); // H -> G+H
  expect(refused(to("step-1-custody", atH), liveSingleG(), atH)).toBe("private_ingest_live_transition"); // G -> H directly
  expect(refused(to("step-1-custody", atG), liveSingleH(), atG)).toBe("private_ingest_live_transition"); // H -> G
  expect(refused(to("step-2-frozen-receipt-overlap", atG), liveSingleG(), atG)).toBe("private_ingest_live_transition");
  expect(refused(to("step-2-frozen-receipt-overlap", atG), liveOverlap(), atG)).toBe("private_ingest_live_transition");
  expect(refused(to("baseline-drain-overlap", atG), liveOverlap(), atG)).toBe("private_ingest_live_transition");
  expect(refused(to("step-2-frozen-receipt", atH), liveOverlap(), atH)).toBe("private_ingest_live_transition");
  expect(refused(to("step-2-frozen-receipt", atG), liveOverlap(), atG)).toBe("private_ingest_live_transition");
  expect(refused(to("step-4-async-on-overlap", atG), liveOverlap("step-1-custody"), atG)).toBe("private_ingest_live_transition");
  // The phase table itself is unchanged: step 1 -> step 4 stays refused at a single generation.
  expect(refused(to("step-4-async-on", atH), liveSingleH(), atH)).toBe("private_ingest_live_transition");
});

function githubFixture(path: string, environment: string) {
  const actor = { id: SCHEMA_RELEASE.dispatcher.id, login: SCHEMA_RELEASE.dispatcher.login, type: "User" };
  const run = { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path,
    run_attempt: 1, actor, triggering_actor: { ...actor }, created_at: new Date().toISOString() };
  return {
    ref: { object: { sha } }, run,
    environment: { id: 7, name: environment, protection_rules: [{ type: "branch_policy" }] as any[],
      deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } },
    branches: { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] },
    runs: { workflow_runs: [{ id: 10, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] },
    jobs: { total_count: 6, jobs: ["checks", "session-read-worker / worker", "unit-tests", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
      .map((name) => ({ name, head_sha: sha, status: "completed", conclusion: "success" })) },
    approvals: [] as any[],
    plan: { ...run, id: Number(planRun), path: P.workflow, status: "completed", conclusion: "success" },
    artifacts: { total_count: 1, artifacts: [{ name: "private-ingest-plan", expired: false }] },
  };
}

function githubGet(f: ReturnType<typeof githubFixture>) {
  return async (path: string): Promise<any> => {
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
}

const context = {
  GITHUB_REPOSITORY: P.repository,
  GITHUB_REPOSITORY_ID: P.repositoryId,
  GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main",
  GITHUB_SHA: sha,
  GITHUB_RUN_ATTEMPT: "1",
  GITHUB_RUN_ID: "100",
};

test("private-ingest GitHub gate binds the profile's overlap into the typed deploy confirmation", async () => {
  const f = githubFixture(P.workflow, P.environment);
  const env = {
    ...context,
    PRIVATE_INGEST_RELEASE_MODE: "deploy",
    PRIVATE_INGEST_RELEASE_SHA: sha,
    PRIVATE_INGEST_RELEASE_PLAN_RUN: planRun,
    PRIVATE_INGEST_RELEASE_CONFIRMATION: `DEPLOY ${P.worker} ${sha} PLAN ${planRun} OVERLAP ${H}`,
  };
  expect((await privateIngestGithubGate(env, githubGet(f), true, H)).mode).toBe("deploy");
  await expect(privateIngestGithubGate(env, githubGet(f), true)).rejects.toThrow("private_ingest_release_confirmation");
});

test("overlap window GitHub gate: dormant refuses, exact pair/confirmation/TTL, exact main, Khaliq's dispatch, protected environment, green CI", async () => {
  const env = (mode: string, ttl = "", confirmation = "") => ({
    ...context, OVERLAP_MODE: mode, OVERLAP_SHA: sha, OVERLAP_TTL_SECONDS: ttl, OVERLAP_CONFIRMATION: confirmation,
  });
  const openEnv = env("open", "3600", `OPEN-OVERLAP ${G} ${H} TTL 3600 SHA ${sha}`);
  const fresh = () => githubFixture(OVERLAP_WORKFLOW, SCHEMA_RELEASE.environment);
  await expect(overlapGithubGate(openEnv, githubGet(fresh()), true)).rejects.toThrow("overlap_operator_dormant");
  expect(await overlapGithubGate(openEnv, githubGet(fresh()), true, atG)).toMatchObject({ mode: "open", from: G, to: H, ttlSeconds: 3600 });
  expect(await overlapGithubGate(env("readback"), githubGet(fresh()), true, atH)).toMatchObject({ mode: "readback", approvalSha256: null });
  expect(await overlapGithubGate(env("close", "", `CLOSE-OVERLAP ${G} ${H} SHA ${sha}`), githubGet(fresh()), true, atH))
    .toMatchObject({ mode: "close" });
  const failures: Array<[Record<string, string>, (f: ReturnType<typeof githubFixture>) => void, string]> = [
    [openEnv, (f) => { f.ref.object.sha = "b".repeat(40); }, "overlap_gate_main_moved"],
    [openEnv, (f) => { f.run.actor.id = 42; }, "overlap_gate_run"],
    [openEnv, (f) => { f.run.triggering_actor.login = "someone"; }, "overlap_gate_run"],
    [openEnv, (f) => { f.run.run_attempt = 2; }, "overlap_gate_run"],
    [openEnv, (f) => { f.run.path = P.workflow; }, "overlap_gate_run"],
    [openEnv, (f) => { f.environment.deployment_branch_policy.protected_branches = true; }, "overlap_gate_environment"],
    [openEnv, (f) => { f.branches.branch_policies.push({ name: "dev", type: "branch" }); f.branches.total_count = 2; }, "overlap_gate_environment_branch"],
    [openEnv, (f) => { f.runs.workflow_runs[0]!.conclusion = "failure"; }, "overlap_gate_ci"],
    [openEnv, (f) => { f.environment.protection_rules.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] }); }, "overlap_gate_human_approval"],
    [{ ...openEnv, GITHUB_RUN_ATTEMPT: "2" }, () => {}, "overlap_gate_context"],
    [{ ...openEnv, GITHUB_SHA: "b".repeat(40) }, () => {}, "overlap_gate_context"],
    [env("open", "59", `OPEN-OVERLAP ${G} ${H} TTL 59 SHA ${sha}`), () => {}, "overlap_operator_ttl"],
    [env("open", "14401", `OPEN-OVERLAP ${G} ${H} TTL 14401 SHA ${sha}`), () => {}, "overlap_operator_ttl"],
    [env("open", "3600", `OPEN-OVERLAP ${G} ${X} TTL 3600 SHA ${sha}`), () => {}, "overlap_operator_confirmation"],
    [env("open", "3600", `OPEN-OVERLAP ${G} ${H} TTL 600 SHA ${sha}`), () => {}, "overlap_operator_confirmation"],
    [env("readback", "60"), () => {}, "overlap_operator_ttl"],
    [env("readback", "", "yes"), () => {}, "overlap_operator_confirmation"],
    [env("reopen"), () => {}, "overlap_operator_inputs"],
  ];
  for (const [input, mutate, expected] of failures) {
    const f = fresh();
    mutate(f);
    await expect(overlapGithubGate(input, githubGet(f), true, atG)).rejects.toThrow(expected);
  }
  // Opening after the contract moved to H is stale.
  await expect(overlapGithubGate(openEnv, githubGet(fresh()), true, atH)).rejects.toThrow("overlap_operator_stale");
});

test("overlap workflow is dispatch-only, gates outside the environment and scopes the existing owner secret to one step", () => {
  const workflow = readFileSync(OVERLAP_WORKFLOW, "utf8");
  const triggers = workflow.slice(workflow.indexOf("\non:"), workflow.indexOf("\npermissions:"));
  expect(triggers).toContain("workflow_dispatch:");
  for (const other of ["push:", "schedule:", "pull_request", "workflow_run", "repository_dispatch"])
    expect(triggers).not.toContain(other);
  expect(workflow).toContain("options: [readback, open, close]");
  expect(workflow.match(/secrets\.[A-Z_]+/gu)).toEqual(["secrets.SUPABASE_MIGRATION_OWNER_URL"]);
  const gate = workflow.slice(workflow.indexOf("  prerequisite-gate:"), workflow.indexOf("  database:"));
  expect(gate).not.toContain("environment:\n");
  expect(gate).toContain("github.ref == 'refs/heads/main'");
  expect(workflow.indexOf("Install without database credentials")).toBeLessThan(workflow.indexOf("secrets.SUPABASE_MIGRATION_OWNER_URL"));
  expect(workflow.indexOf("ingest-generation-overlap-github.ts --approved")).toBeLessThan(workflow.indexOf("secrets.SUPABASE_MIGRATION_OWNER_URL"));
  expect(workflow).toContain("echo \"environment=session-schema-production\"");
  expect(workflow).toContain("persist-credentials: false");
  expect(workflow).not.toContain("admit-during-apply");
  expect(readFileSync(".github/workflows/apply-session-schema.yml", "utf8")).toContain("SUPABASE_MIGRATION_OWNER_URL: ${{ secrets.SUPABASE_MIGRATION_OWNER_URL }}");
});

test("the retained plan unbinds INGEST_ACCEPT_GENERATIONS only for G+H -> H, or G+H -> G under the reviewed pair", () => {
  const profile = (expectedGeneration: string, extra: Record<string, unknown> = {}) =>
    ({ ...tracked.sessionIngest, kmsKeyArn, expectedGeneration, ...extra }) as CloudflareStageInputs["sessionIngest"];
  const live = (accepted: string) => ({ INGEST_EXPECT_GENERATION: G, INGEST_ACCEPT_GENERATIONS: accepted });
  const proposed = (expected: string, extra: Record<string, unknown> = {}) => ({ INGEST_EXPECT_GENERATION: expected, ...extra });
  // Closure G+H -> H: unchanged and contract-independent.
  for (const contract of [PRIVATE_INGEST_GENERATION_CONTRACT, atG, atH])
    expect(acceptGenerationsDeletionAllowed(profile(H), live(`${G},${H}`), proposed(H), contract)).toBe(true);
  expect(acceptGenerationsDeletionAllowed(profile(H), live(`${H},${G}`), proposed(H), atH)).toBe(false);
  // Rollback G+H -> G: only under a reviewed pair still bound at G, with the exact live accept set.
  expect(acceptGenerationsDeletionAllowed(profile(G), live(`${G},${H}`), proposed(G), atG)).toBe(true);
  expect(acceptGenerationsDeletionAllowed(profile(G), live(`${G},${H}`), proposed(G))).toBe(false);
  expect(acceptGenerationsDeletionAllowed(profile(G), live(`${G},${H}`), proposed(G), atH)).toBe(false);
  expect(acceptGenerationsDeletionAllowed(profile(G), live(`${G},${X}`), proposed(G), atG)).toBe(false);
  expect(acceptGenerationsDeletionAllowed(profile(G), live(`${H},${G}`), proposed(G), atG)).toBe(false);
  expect(acceptGenerationsDeletionAllowed(profile(G, { overlapGeneration: H }), live(`${G},${H}`), proposed(G), atG)).toBe(false);
  expect(acceptGenerationsDeletionAllowed(profile(G), live(`${G},${H}`), proposed(G, { INGEST_ACCEPT_GENERATIONS: `${G},${H}` }), atG)).toBe(false);
  expect(acceptGenerationsDeletionAllowed(profile(H), live(`${G},${H}`), proposed(G), atG)).toBe(false);
  expect(acceptGenerationsDeletionAllowed(profile(G), undefined, proposed(G), atG)).toBe(false);
});

// step-0-shadow composed with the generation overlap: step-0-shadow-overlap is step-0-shadow's twin,
// and the mandatory baseline-drain <-> step-0-shadow <-> step-1-custody graph still holds at every
// generation state.
type Phase = Parameters<typeof privateIngestProfileForPhase>[0];
const at = (phase: Phase, contract: PrivateIngestGenerationContract) => privateIngestProfileForPhase(phase, kmsKeyArn, contract);
const liveAt = (phase: Phase, contract: PrivateIngestGenerationContract) => liveFixture(at(phase, contract));
const move = (target: CloudflareStageInputs, live: ReturnType<typeof liveFixture>, contract: PrivateIngestGenerationContract) =>
  privateIngestTransitionReadback(target, live.active, live.version, contract);
const moveCode = (target: CloudflareStageInputs, live: ReturnType<typeof liveFixture>, contract: PrivateIngestGenerationContract) =>
  code(() => move(target, live, contract));

test("step-0-shadow-overlap: tracked profile, phase builder and live candidate are shadow custody with exactly {G, H}", () => {
  const tracked0 = withTarget({ ...tracked.sessionIngest, custodySource: "shadow", overlapGeneration: H });
  expect(privateIngestReleasePhase(tracked0, atG)).toBe("step-0-shadow-overlap");
  const built = at("step-0-shadow-overlap", atG);
  expect(built.sessionIngest).toMatchObject({ custodySource: "shadow", asyncAdmission: "drain", expectedGeneration: G, overlapGeneration: H });
  expect(built.sessionIngest.syncAdmissionFrozenReceipt).toBeUndefined();
  const vars = sessionIngestVars(built.sessionIngest);
  expect(vars.INGEST_CUSTODY_SOURCE).toBe("shadow");
  expect(vars.INGEST_ACCEPT_GENERATIONS).toBe(`${G},${H}`);
  expect(vars.INGEST_EXPECT_GENERATION).toBe(G);
  // The overlap twin cannot carry the receipt bridge or async admission (still shadow's drain-only shape).
  for (const extra of [{ syncAdmissionFrozenReceipt: "on" }, { asyncAdmission: "on" }])
    expect(code(() => privateIngestReleasePhase(withTarget({ ...tracked0.sessionIngest, ...extra }), atG)))
      .toBe("private_ingest_profile_phase");
  // Stale and unexpected pairs refuse exactly as for the other base phases.
  expect(code(() => at("step-0-shadow-overlap", atH))).toBe("private_ingest_profile_overlap_stale");
  expect(code(() => privateIngestReleasePhase(withTarget({ ...tracked0.sessionIngest, overlapGeneration: X }), atG)))
    .toBe("private_ingest_profile_overlap_unexpected");
  // The live step-0-shadow-overlap version is one unambiguous candidate (same-state redeploy is a no-move).
  expect(move(built, liveAt("step-0-shadow-overlap", atG), atG)).toMatchObject({
    from: "step-0-shadow-overlap", to: "step-0-shadow-overlap", generationFrom: "G+H", generationTo: "G+H",
  });
  // A live shadow-overlap version reads back as neither the step-0-shadow nor the step-1-custody-overlap target.
  expect(moveCode(at("step-1-custody-overlap", atG), liveAt("step-0-shadow-overlap", atG), atG)).toBe("private_ingest_live_transition");
});

test("step-0-shadow-overlap is reachable only beside step-0-shadow: open G -> G+H, finish G+H -> H, rollback G+H -> G", () => {
  expect(move(at("step-0-shadow-overlap", atG), liveAt("step-0-shadow", atG), atG))
    .toMatchObject({ from: "step-0-shadow", to: "step-0-shadow-overlap", generationFrom: "G", generationTo: "G+H" });
  expect(move(at("step-0-shadow", atH), liveAt("step-0-shadow-overlap", atG), atH))
    .toMatchObject({ from: "step-0-shadow-overlap", to: "step-0-shadow", generationFrom: "G+H", generationTo: "H" });
  expect(move(at("step-0-shadow", atG), liveAt("step-0-shadow-overlap", atG), atG))
    .toMatchObject({ from: "step-0-shadow-overlap", to: "step-0-shadow", generationFrom: "G+H", generationTo: "G" });
  // After the window, the single-generation shadow graph applies again at H.
  expect(move(at("step-1-custody", atH), liveAt("step-0-shadow", atH), atH))
    .toMatchObject({ from: "step-0-shadow", to: "step-1-custody", generationFrom: "H", generationTo: "H" });
  expect(move(at("step-0-shadow", atH), liveAt("baseline-drain", atH), atH))
    .toMatchObject({ from: "baseline-drain", to: "step-0-shadow" });
  expect(move(at("step-0-shadow", atH), liveAt("step-1-custody", atH), atH))
    .toMatchObject({ from: "step-1-custody", to: "step-0-shadow" });
  // Not reachable from any other phase, overlapped or not.
  for (const [live, contract] of [
    ["baseline-drain", atG], ["step-1-custody", atG], ["step-2-frozen-receipt", atG], ["step-4-async-on", atG],
    ["baseline-drain-overlap", atG], ["step-1-custody-overlap", atG], ["step-2-frozen-receipt-overlap", atG],
  ] as const)
    expect(moveCode(at("step-0-shadow-overlap", atG), liveAt(live, contract), contract)).toBe("private_ingest_live_transition");
  // And it cannot leave step-0-shadow's base while {G, H} is live.
  for (const target of ["baseline-drain", "step-1-custody", "baseline-drain-overlap", "step-1-custody-overlap"] as const)
    expect(moveCode(at(target, atG), liveAt("step-0-shadow-overlap", atG), atG)).toBe("private_ingest_live_transition");
  for (const target of ["baseline-drain", "step-1-custody", "step-2-frozen-receipt"] as const)
    expect(moveCode(at(target, atH), liveAt("step-0-shadow-overlap", atG), atH)).toBe("private_ingest_live_transition");
});

test("with an overlap present, no transition skips step-0-shadow between baseline-drain and step-1-custody", () => {
  // Direct baseline -> custody (and the reverse) at every generation state and through every overlap edge.
  const refusals: Array<[Phase, Phase, PrivateIngestGenerationContract]> = [
    ["baseline-drain", "step-1-custody", atG],
    ["baseline-drain", "step-1-custody", atH],
    ["step-1-custody", "baseline-drain", atG],
    ["step-1-custody", "baseline-drain", atH],
    ["baseline-drain", "step-1-custody-overlap", atG],
    ["baseline-drain-overlap", "step-1-custody-overlap", atG],
    ["baseline-drain-overlap", "step-1-custody", atG],
    ["step-1-custody-overlap", "baseline-drain-overlap", atG],
    ["step-1-custody-overlap", "baseline-drain", atG],
    ["step-1-custody", "baseline-drain-overlap", atG],
  ];
  for (const [live, target, contract] of refusals) {
    const liveContract = live.endsWith("-overlap") ? atG : contract;
    expect(moveCode(at(target, contract), liveAt(live, liveContract), contract)).toBe("private_ingest_live_transition");
  }
  // The rollback G+H -> H after the window lands at the base the overlap sat on; it never lands on custody.
  expect(moveCode(at("step-1-custody", atH), liveAt("baseline-drain-overlap", atG), atH)).toBe("private_ingest_live_transition");
  expect(move(at("baseline-drain", atH), liveAt("baseline-drain-overlap", atG), atH))
    .toMatchObject({ from: "baseline-drain-overlap", to: "baseline-drain", generationTo: "H" });
  // Dormant contract keeps #558's single-generation graph byte-for-byte.
  const dormant = (phase: Phase) => privateIngestProfileForPhase(phase, kmsKeyArn);
  const liveDormant = (phase: Phase) => liveFixture(dormant(phase));
  expect(code(() => privateIngestTransitionReadback(dormant("step-1-custody"), liveDormant("baseline-drain").active, liveDormant("baseline-drain").version)))
    .toBe("private_ingest_live_transition");
  expect(Object.keys(privateIngestTransitionReadback(dormant("step-1-custody"), liveDormant("step-0-shadow").active, liveDormant("step-0-shadow").version)))
    .toEqual(["status", "from", "to", "deploymentId", "versionId"]);
});

test("the shadow/custody CI hooks only fire on the exact single-generation edge, and the DB window stays admitDuringApply=false", () => {
  // scripts/private-ingest-ci.ts gates the SHADOW receipt on from=step-0-shadow,to=step-1-custody and the
  // RHC witness on the reverse edge. No overlap-phase transition can produce either pair of labels.
  const pairs: string[] = [];
  const phases: Phase[] = ["baseline-drain", "step-0-shadow", "step-1-custody", "step-2-frozen-receipt", "step-4-async-on"];
  const states: Array<[Phase, PrivateIngestGenerationContract]> = [];
  for (const base of phases) states.push([base, atG], [`${base}-overlap`, atG], [base, atH]);
  for (const [livePhase, liveContract] of states)
    for (const [targetPhase, targetContract] of states) {
      if (liveContract !== targetContract && !(livePhase.endsWith("-overlap"))) continue;
      try {
        const r = move(at(targetPhase, targetContract), liveAt(livePhase, liveContract), targetContract);
        pairs.push(`${r.from}>${r.to}`);
      } catch { /* refused */ }
    }
  expect(pairs).toContain("step-0-shadow>step-1-custody");
  expect(pairs).toContain("step-1-custody>step-0-shadow");
  expect(pairs).toContain("step-0-shadow>step-0-shadow-overlap");
  for (const pair of pairs) {
    const [from, to] = pair.split(">") as [string, string];
    if (from.endsWith("-overlap") || to.endsWith("-overlap"))
      expect(from.replace(/-overlap$/u, "")).toBe(to.replace(/-overlap$/u, ""));
    expect(pair).not.toBe("baseline-drain>step-1-custody");
    expect(pair).not.toBe("step-1-custody>baseline-drain");
  }
  const ci = readFileSync("scripts/private-ingest-ci.ts", "utf8");
  expect(ci).toContain('transition.from === "step-0-shadow" && transition.to === "step-1-custody"');
  expect(ci).toContain('transition.from === "step-1-custody" && transition.to === "step-0-shadow"');
  expect(ci).toContain("privateIngestReleaseInputs(process.env, overlapGeneration)");
  // The DB window operator never admits the old generation during the apply, whatever the Worker phase.
  const operator = readFileSync("src/hoopdb/ingest-generation-overlap-operator.ts", "utf8");
  expect(operator.match(/admitDuringApply: *[a-z]+/gu)).toEqual(["admitDuringApply: false"]);
  expect(operator).toContain("mine.admitDuringApply !== false");
  expect(Object.keys(at("step-0-shadow-overlap", atG).sessionIngest)).not.toContain("admitDuringApply");
});

test("the deploy confirmation for step-0-shadow-overlap is the base confirmation plus OVERLAP <H>, never a bare SHADOW", () => {
  const deploy = (confirmation: string) => ({
    PRIVATE_INGEST_RELEASE_MODE: "deploy",
    PRIVATE_INGEST_RELEASE_SHA: sha,
    PRIVATE_INGEST_RELEASE_PLAN_RUN: planRun,
    PRIVATE_INGEST_RELEASE_CONFIRMATION: confirmation,
  });
  const base = `DEPLOY ${P.worker} ${sha} PLAN ${planRun}`;
  expect(privateIngestReleaseInputs(deploy(`${base} OVERLAP ${H}`), H).mode).toBe("deploy");
  // REVIEWED_CUSTODY_SHADOW_MATCH is null at this revision, so no SHADOW form is accepted at all.
  for (const bad of [`${base} SHADOW ${"a".repeat(64)} OVERLAP ${H}`, `${base} OVERLAP ${H} SHADOW ${"a".repeat(64)}`, base])
    expect(code(() => privateIngestReleaseInputs(deploy(bad), H))).toBe("private_ingest_release_confirmation");
});
