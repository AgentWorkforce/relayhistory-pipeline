import { test, expect } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import {
  releaseInputs,
  releaseConnection,
  SCHEMA_RELEASE as P,
} from "../schema-release-contract.ts";
import { githubReleaseGate } from "../../../scripts/schema-release-github.ts";
import {
  validateBootstrapPlan,
  validateReleasePlan,
} from "../../../scripts/schema-release-database.ts";
import {
  manifestGeneration,
  migrationTargetSplit,
  RELEASE_TREE,
  releaseManifestDigest,
} from "../migrate.ts";
import { createHash } from "node:crypto";
import {
  bootstrapApproval,
  bootstrapChainDigest,
  bootstrapContractDigest,
  bootstrapChainRoles,
  bootstrapFilesToRun,
  bootstrapReadbackVerdict,
  bootstrapSection,
  bootstrapTargetChain,
  chainSqlTopLevel,
  requireBootstrapRoles,
  requireBootstrapTargetChain,
  requireSameBootstrapPlan,
  targetRequiredRoles,
  requireTransactionSafeChainSql,
  roleContractProblems,
  stripPsqlMeta,
  validateBootstrapSection,
  type BootstrapEdge,
  type BootstrapState,
} from "../schema-release-bootstrap.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { SESSION_BOOTSTRAP_ROLES } from "../session-bootstrap-roles.ts";
import {
  refreshReleaseBoundary,
  releaseBoundaryDigest,
  requireSafeReleaseBoundary,
} from "../schema-release-boundary.ts";
const sha = "a".repeat(40),
  generation = "b".repeat(64),
  target = "upstream:20261105047000_coordination_fact_readers.sql",
  // target_sha: the full commit (sha itself or on sha's first-parent main history) whose
  // tree ends at the target; its own push CI and pg-proof runs must be green.
  targetSha = "9".repeat(40),
  // The plan's targetGeneration (target_sha's own generation); differs from generation
  // when held.
  targetGeneration = "f".repeat(64),
  // The plan's canonical manifest digest, carried only inside the typed approval.
  manifestDigest = "8".repeat(64);
const base = {
  RELEASE_MODE: "plan",
  RELEASE_SHA: sha,
  RELEASE_GENERATION: generation,
  RELEASE_BOUNDARY: "global-default-revoke",
  RELEASE_TARGET: target,
  RELEASE_TARGET_SHA: targetSha,
  GITHUB_REPOSITORY: P.repository,
  GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main",
  GITHUB_SHA: sha,
  GITHUB_RUN_ATTEMPT: "1",
  GITHUB_RUN_ID: "100",
};
const url = `postgresql://${P.owner}.${P.project}:synthetic-only@${P.host}:5432/postgres?sslmode=require`;
test("strict pooler/project/owner pins and URL options cannot weaken TLS", () => {
  expect(releaseConnection(url).host).toBe(P.host);
  for (const bad of [
    url.replace(P.project, "foreign"),
    url.replace(P.owner, "foreign_owner"),
    url.replace(P.host, "evil.example"),
    url.replace(":5432/", ":6543/"),
    url.replace("/postgres?", "/other?"),
    url + "&sslmode=disable",
    url + "&sslrootcert=anything",
    url + "&options=-c%20role%3Dpostgres",
    url + "&ssl=true",
    url + "#fragment",
    url.replace(":synthetic-only", ""),
  ])
    expect(() => releaseConnection(bad)).toThrow();
});
test("typed approval binds source, generation, project and prior plan", () => {
  const apply = {
    ...base,
    RELEASE_MODE: "apply",
    RELEASE_PLAN_RUN: "90",
    RELEASE_TARGET_GENERATION: targetGeneration,
    RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
  };
  expect(releaseInputs(apply).mode).toBe("apply");
  for (const [k, v] of Object.entries({
    RELEASE_SHA: "c".repeat(40),
    RELEASE_GENERATION: "d".repeat(64),
    RELEASE_PLAN_RUN: "91",
    RELEASE_TARGET_GENERATION: "e".repeat(64),
    RELEASE_TARGET_SHA: "7".repeat(40),
    RELEASE_APPROVAL: "yes",
    RELEASE_BOUNDARY: "none",
  }))
    expect(() => releaseInputs({ ...apply, [k]: v })).toThrow();
  expect(() =>
    releaseInputs({ ...base, RELEASE_APPROVAL: "anything" }),
  ).toThrow();
});
// The operator never chooses a function boundary: global-default-revoke is the
// required pre-refresh control and CI refreshes/verifies the event trigger itself.
const refusedBoundaries = [
  "event-trigger",
  "EVENT-TRIGGER",
  "event_trigger",
  "Global-Default-Revoke",
  " global-default-revoke",
  "global-default-revoke ",
  "global-default-revoke,event-trigger",
  "global-default-revoke\nevent-trigger",
  "none",
  "",
  undefined,
];
test("release contract refuses every boundary but global-default-revoke in plan and apply before any GitHub or database work", async () => {
  const apply = {
    ...base,
    RELEASE_MODE: "apply",
    RELEASE_PLAN_RUN: "90",
    RELEASE_TARGET_GENERATION: targetGeneration,
    RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
  };
  for (const env of [base, apply]) {
    expect(releaseInputs(env).boundary).toBe("global-default-revoke");
    for (const RELEASE_BOUNDARY of refusedBoundaries) {
      const input = { ...env, RELEASE_BOUNDARY };
      expect(() => releaseInputs(input)).toThrow("release_inputs");
      let calls = 0;
      const counting = async (path: string) => (calls++, get(api())(path));
      // Both workflow jobs and the database step enter through this gate first.
      for (const approved of [false, true])
        await expect(
          githubReleaseGate(input, counting, approved),
        ).rejects.toMatchObject({ code: "release_inputs" });
      expect(calls).toBe(0);
    }
  }
});
function api() {
  const run = {
    id: 100,
    head_sha: sha,
    head_branch: "main",
    event: "workflow_dispatch",
    path: P.workflow,
    run_attempt: 1,
  };
  const env = {
    id: 7,
    name: P.environment,
    protection_rules: [
      {
        type: "required_reviewers",
        reviewers: [{ type: "User", reviewer: { type: "User", id: 1724137 } }],
      },
    ],
    deployment_branch_policy: {
      protected_branches: false,
      custom_branch_policies: true,
    },
  };
  return {
    ref: { object: { sha } },
    run,
    env,
    branches: {
      total_count: 1,
      branch_policies: [{ name: "main", type: "branch" }],
    },
    approvals: [
      {
        state: "approved",
        user: { type: "User", id: 1724137 },
        environments: [{ id: 7, name: P.environment }],
      },
    ],
    ci: {
      workflow_runs: [
        {
          id: 10,
          head_sha: sha,
          head_branch: "main",
          event: "push",
          status: "completed",
          conclusion: "success",
          run_attempt: 1,
        },
      ],
    },
    pg: {
      workflow_runs: [
        {
          id: 11,
          head_sha: sha,
          head_branch: "main",
          event: "push",
          status: "completed",
          conclusion: "success",
          run_attempt: 1,
        },
      ],
    },
    jobs10: {
      total_count: 2,
      jobs: ["checks", "session-read-worker / worker"].map((name) => ({
        name,
        head_sha: sha,
        status: "completed",
        conclusion: "success",
      })),
    },
    jobs11: {
      total_count: 3,
      jobs: [15, 16, 17].map((v) => ({
        name: "real PostgreSQL " + v,
        head_sha: sha,
        status: "completed",
        conclusion: "success",
      })),
    },
    // target_sha's own push runs on main (ids 20/21), checked when it differs from sha.
    tci: {
      workflow_runs: [
        {
          id: 20,
          head_sha: targetSha,
          head_branch: "main",
          event: "push",
          status: "completed",
          conclusion: "success",
          run_attempt: 1,
        },
      ],
    },
    tpg: {
      workflow_runs: [
        {
          id: 21,
          head_sha: targetSha,
          head_branch: "main",
          event: "push",
          status: "completed",
          conclusion: "success",
          run_attempt: 1,
        },
      ],
    },
    jobs20: {
      total_count: 2,
      jobs: ["checks", "session-read-worker / worker"].map((name) => ({
        name,
        head_sha: targetSha,
        status: "completed",
        conclusion: "success",
      })),
    },
    jobs21: {
      total_count: 3,
      jobs: [15, 16, 17].map((v) => ({
        name: "real PostgreSQL " + v,
        head_sha: targetSha,
        status: "completed",
        conclusion: "success",
      })),
    },
    plan: {
      ...run,
      id: 90,
      status: "completed",
      conclusion: "success",
      created_at: new Date().toISOString(),
    },
  };
}
type Fixture = ReturnType<typeof api>;
function get(f: Fixture) {
  return async (path: string): Promise<any> => {
    if (path.endsWith("/git/ref/heads/main")) return f.ref;
    if (path.endsWith("/actions/runs/100")) return f.run;
    if (path.endsWith("/actions/runs/90")) return f.plan;
    if (path.endsWith("/environments/" + P.environment)) return f.env;
    if (path.includes("deployment-branch-policies")) return f.branches;
    if (path.endsWith("/approvals")) return f.approvals;
    const target = path.includes(`head_sha=${targetSha}&`);
    if (path.includes("/workflows/ci.yml/")) return target ? f.tci : f.ci;
    if (path.includes("/workflows/pg-proof.yml/")) return target ? f.tpg : f.pg;
    if (path.includes("/runs/10/")) return f.jobs10;
    if (path.includes("/runs/11/")) return f.jobs11;
    if (path.includes("/runs/20/")) return f.jobs20;
    if (path.includes("/runs/21/")) return f.jobs21;
    throw new Error("unexpected " + path);
  };
}
test("real main, both exact-head workflows/jobs and actual named human approval required", async () => {
  expect((await githubReleaseGate(base, get(api()), true)).sha).toBe(sha);
});
test("pinned human dispatch is the fail-closed fallback when the plan has no environment reviewers", async () => {
  const f = api();
  f.env.protection_rules = [];
  f.run.actor = { type: "User", ...P.dispatcher };
  f.run.triggering_actor = { type: "User", ...P.dispatcher };
  expect((await githubReleaseGate(base, get(f), true)).sha).toBe(sha);
  for (const mutate of [
    (x: Fixture) => (x.run.actor.id = P.dispatcher.id + 1),
    (x: Fixture) => (x.run.actor.login = "foreign"),
    (x: Fixture) => (x.run.actor.type = "Bot"),
    (x: Fixture) => (x.run.triggering_actor.id = P.dispatcher.id + 1),
    (x: Fixture) => (x.run.triggering_actor.login = "foreign"),
    (x: Fixture) => (x.run.triggering_actor.type = "Bot"),
  ]) {
    const refused = api();
    refused.env.protection_rules = [];
    refused.run.actor = { type: "User", ...P.dispatcher };
    refused.run.triggering_actor = { type: "User", ...P.dispatcher };
    mutate(refused);
    await expect(githubReleaseGate(base, get(refused), true)).rejects.toThrow();
  }
});
test("a reviewer rule never falls back to dispatcher identity", async () => {
  const noApproval = api();
  noApproval.run.actor = { type: "User", ...P.dispatcher };
  noApproval.run.triggering_actor = { type: "User", ...P.dispatcher };
  noApproval.approvals = [];
  await expect(
    githubReleaseGate(base, get(noApproval), true),
  ).rejects.toThrow("release_human_approval");

  const teamOnly = api();
  teamOnly.run.actor = { type: "User", ...P.dispatcher };
  teamOnly.run.triggering_actor = { type: "User", ...P.dispatcher };
  teamOnly.env.protection_rules[0]!.reviewers = [
    { type: "Team", reviewer: { type: "Team", id: 99 } },
  ];
  await expect(
    githubReleaseGate(base, get(teamOnly), true),
  ).rejects.toThrow("release_environment");
});
test("missing protection metadata and GitHub API failure refuse closed", async () => {
  const missing = api();
  delete (missing.env as any).protection_rules;
  await expect(githubReleaseGate(base, get(missing), true)).rejects.toThrow(
    "release_environment",
  );
  await expect(
    githubReleaseGate(base, async () => {
      throw new Error("synthetic API failure");
    }, true),
  ).rejects.toThrow("synthetic API failure");
});
const mutations: Array<[string, (f: Fixture) => void]> = [
  ["main moved", (f) => (f.ref.object.sha = "e".repeat(40))],
  ["wrong workflow", (f) => (f.run.path = ".github/workflows/untrusted.yml")],
  ["missing human rule", (f) => (f.env.protection_rules = [])],
  [
    "team-only reviewer",
    (f) => (f.env.protection_rules[0]!.reviewers[0]!.type = "Team"),
  ],
  [
    "unrestricted branch",
    (f) => (f.env.deployment_branch_policy.custom_branch_policies = false),
  ],
  ["branch pattern", (f) => (f.branches.branch_policies[0]!.name = "*")],
  ["tag main", (f) => (f.branches.branch_policies[0]!.type = "tag")],
  ["ci missing", (f) => (f.ci.workflow_runs = [])],
  ["ci running", (f) => (f.ci.workflow_runs[0]!.status = "in_progress")],
  ["ci failed", (f) => (f.ci.workflow_runs[0]!.conclusion = "failure")],
  ["wrong tested SHA", (f) => (f.jobs10.jobs[0]!.head_sha = "e".repeat(40))],
  [
    "worker omitted",
    (f) => {
      f.jobs10.jobs.pop();
      f.jobs10.total_count--;
    },
  ],
  ["PG16 skipped", (f) => (f.jobs11.jobs[1]!.conclusion = "skipped")],
  ["job truncated", (f) => (f.jobs11.total_count = 101)],
  ["bypassed human gate", (f) => (f.approvals = [])],
  ["bot approval", (f) => (f.approvals[0]!.user.type = "Bot")],
  ["foreign human", (f) => (f.approvals[0]!.user.id = 123)],
  ["different environment", (f) => (f.approvals[0]!.environments[0]!.id = 8)],
];
for (const [label, mutate] of mutations)
  test(label + " refuses", async () => {
    const f = api();
    mutate(f);
    await expect(githubReleaseGate(base, get(f), true)).rejects.toThrow();
  });
for (const [k, v] of Object.entries({
  GITHUB_REF: "refs/heads/feature",
  GITHUB_RUN_ATTEMPT: "2",
  GITHUB_EVENT_NAME: "pull_request",
  GITHUB_REPOSITORY: "foreign/repo",
  GITHUB_SHA: "e".repeat(40),
}))
  test(
    k + " refuses",
    async () =>
      await expect(
        githubReleaseGate({ ...base, [k]: v }, get(api()), true),
      ).rejects.toThrow(),
  );
test("release gate pins the renamed repository and reads only its API", async () => {
  expect(P.repository).toBe("AgentWorkforce/relayhistory-pipeline");
  const paths: string[] = [];
  const inner = get(api());
  await githubReleaseGate(base, async (path) => (paths.push(path), inner(path)), true);
  expect(paths.length).toBeGreaterThan(0);
  expect(paths.every((p) => p.startsWith("/repos/AgentWorkforce/relayhistory-pipeline/"))).toBe(true);
  for (const name of [
    "AgentWorkforce/sessions-pipeline",
    "AgentWorkforce/sessions-pipeline-oss",
    "fork/relayhistory-pipeline",
    "agentworkforce/relayhistory-pipeline",
  ])
    await expect(
      githubReleaseGate({ ...base, GITHUB_REPOSITORY: name }, get(api()), true),
    ).rejects.toThrow("release_context");
});
const targetCiMutations: [string, (f: Fixture) => void, string][] = [
  ["target ci missing", (f) => (f.tci.workflow_runs = []), "release_target_ci"],
  ["target pg-proof missing", (f) => (f.tpg.workflow_runs = []), "release_target_ci"],
  ["target ci running", (f) => (f.tci.workflow_runs[0]!.status = "in_progress"), "release_target_ci"],
  ["target pg-proof failed", (f) => (f.tpg.workflow_runs[0]!.conclusion = "failure"), "release_target_ci"],
  // A run listed for another commit or off main or not a push does not count.
  ["target run of another sha", (f) => (f.tci.workflow_runs[0]!.head_sha = sha), "release_target_ci"],
  ["target run off main", (f) => (f.tpg.workflow_runs[0]!.head_branch = "feature"), "release_target_ci"],
  // The latest run decides: an older green run does not mask a newer failed one.
  [
    "target latest run failed",
    (f) => f.tci.workflow_runs.push({ ...f.tci.workflow_runs[0]!, id: 30, conclusion: "failure" }),
    "release_target_ci",
  ],
  ["target job failed", (f) => (f.jobs20.jobs[0]!.conclusion = "failure"), "release_target_ci_jobs"],
  ["target job not completed", (f) => (f.jobs21.jobs[2]!.status = "in_progress"), "release_target_ci_jobs"],
  ["target job of another sha", (f) => (f.jobs21.jobs[0]!.head_sha = sha), "release_target_ci_jobs"],
  [
    "target required job missing",
    (f) => {
      f.jobs20.jobs.pop();
      f.jobs20.total_count--;
    },
    "release_target_ci_jobs",
  ],
  ["target jobs truncated", (f) => (f.jobs21.total_count = 101), "release_target_ci_jobs"],
];
for (const [label, mutate, expected] of targetCiMutations)
  test(label + " refuses with " + expected + " in plan and apply", async () => {
    const apply = {
      ...base,
      RELEASE_MODE: "apply",
      RELEASE_PLAN_RUN: "90",
      RELEASE_TARGET_GENERATION: targetGeneration,
      RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
    };
    for (const env of [base, apply]) {
      const f = api();
      mutate(f);
      await expect(githubReleaseGate(env, get(f), true)).rejects.toMatchObject({ code: expected });
    }
  });
test("target_sha's own push CI and pg-proof runs are read and must be green when it differs from sha", async () => {
  const paths: string[] = [];
  const inner = get(api());
  expect((await githubReleaseGate(base, async (p) => (paths.push(p), inner(p)), true)).targetSha).toBe(targetSha);
  for (const wf of ["ci.yml", "pg-proof.yml"]) {
    expect(paths).toContain(`/repos/${P.repository}/actions/workflows/${wf}/runs?event=push&head_sha=${sha}&per_page=100`);
    expect(paths).toContain(`/repos/${P.repository}/actions/workflows/${wf}/runs?event=push&head_sha=${targetSha}&per_page=100`);
  }
  expect(paths.some((p) => p.includes("/actions/runs/20/attempts/1/jobs"))).toBe(true);
  expect(paths.some((p) => p.includes("/actions/runs/21/attempts/1/jobs"))).toBe(true);
  // The release sha's check still runs first and keeps its own codes.
  const f = api();
  f.ci.workflow_runs = [];
  f.tci.workflow_runs = [];
  await expect(githubReleaseGate(base, get(f), true)).rejects.toMatchObject({ code: "release_ci" });
  const g = api();
  g.jobs11.jobs[0]!.conclusion = "failure";
  await expect(githubReleaseGate(base, get(g), true)).rejects.toMatchObject({ code: "release_ci_jobs" });
});
test("target_sha === sha: no extra GitHub calls for the target", async () => {
  const paths: string[] = [];
  const f = api();
  // Any read of the target fixtures would refuse.
  f.tci.workflow_runs = [];
  f.tpg.workflow_runs = [];
  const inner = get(f);
  const same = { ...base, RELEASE_TARGET_SHA: sha };
  expect((await githubReleaseGate(same, async (p) => (paths.push(p), inner(p)), true)).targetSha).toBe(sha);
  expect(paths.filter((p) => p.includes(`head_sha=${targetSha}`))).toEqual([]);
  expect(paths.filter((p) => p.includes("/runs/20/") || p.includes("/runs/21/"))).toEqual([]);
  // Exactly one listing per required workflow (the release sha's).
  expect(paths.filter((p) => p.includes("/actions/workflows/")).length).toBe(2);
});
test("apply rejects foreign, stale, replayed or failed prior plan and artifact bindings", async () => {
  const env = {
    ...base,
    RELEASE_MODE: "apply",
    RELEASE_PLAN_RUN: "90",
    RELEASE_TARGET_GENERATION: targetGeneration,
    RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
  };
  expect((await githubReleaseGate(env, get(api()), true)).mode).toBe("apply");
  for (const values of [
    { head_sha: "e".repeat(40) },
    { run_attempt: 2 },
    { conclusion: "failure" },
    { created_at: new Date(Date.now() - 25 * 3600000).toISOString() },
    { path: "wrong" },
  ]) {
    const f = api();
    Object.assign(f.plan, values);
    await expect(githubReleaseGate(env, get(f), true)).rejects.toThrow();
  }
  // The target commit's manifest (release-target.ts shape): its schema steps through the
  // target and its 7 verifier digests; the plan's targetGeneration and manifestDigest are
  // computed from it with the one canonical definition.
  const prior = "upstream:20261105046010_session_directory_read_api_query.sql";
  const targetManifest = {
    targetSha,
    target,
    steps: [[prior, "1".repeat(64)], [target, "2".repeat(64)]] as [string, string][],
    verifiers: RELEASE_TREE.verifiers.map((v, i) => [v.name, String(i + 3).repeat(64)]) as [string, string][],
  };
  const planGeneration = manifestGeneration(targetManifest);
  const planDigest = releaseManifestDigest(targetManifest);
  const input = {
    sha,
    generation,
    boundary: "global-default-revoke",
    target,
    targetSha,
    targetGeneration: planGeneration,
    manifestDigest: planDigest,
    planRun: "90",
  };
  const existingBoundary = {
    present: true,
    event: "ddl_command_end",
    tags_exact: true,
    enabled_ok: true,
    owner: P.owner,
    owner_is_migration_owner: true,
    function_is_reviewed: true,
    function_owner: P.owner,
    function_owner_is_migration_owner: true,
    schema_owner_is_migration_owner: true,
    prosrc_sha256: "d".repeat(64),
    proconfig_exact: true,
    security_definer: false,
    volatility: "v",
    language: "plpgsql",
    returns: "event_trigger",
    function_acl: ["<owner>:EXECUTE"],
    function_public_execute: false,
    runtime_role_setting_matches: true,
    schema_private: true,
  };
  const plan = {
    version: 1,
    project: P.project,
    sha,
    generation,
    boundary: "global-default-revoke",
    runId: "90",
    ledgerSha256: "c".repeat(64),
    inventory: [{}],
    existingBoundary,
    target,
    expectedPending: [prior, target],
    held: ["upstream:20261105050000_operator_session_erase.sql"],
    targetSha,
    targetGeneration: planGeneration,
    manifestDigest: planDigest,
    targetManifest,
  };
  expect(validateReleasePlan(plan, input)).toEqual({
    ledgerSha256: "c".repeat(64),
    existingBoundary,
    expectedPending: plan.expectedPending,
    held: plan.held,
    targetGeneration: planGeneration,
    targetSha,
    manifestDigest: planDigest,
  });
  // Bootstrap mode consumes the same plan run: the release plan as apply validates it,
  // plus its bootstrap section bound to the typed approval's CHAIN and ROLES.
  {
    const section = bsSection({ files: bsFiles(), state: bsState([admit]), supabaseMode: true });
    const bInput = { sha, generation, boundary: "global-default-revoke", target, targetSha, targetGeneration: planGeneration, planRun: "90", chainDigest: section.chain_digest, bootstrapRoles: [admit] };
    const withSection = JSON.parse(JSON.stringify({ ...plan, bootstrap: section }));
    expect(validateBootstrapPlan(withSection, bInput).section).toEqual(section);
    expect(validateBootstrapPlan(withSection, bInput).release.held).toEqual(plan.held);
    for (const change of [{ chainDigest: "0".repeat(64) }, { bootstrapRoles: [] }, { bootstrapRoles: [admit, "bulls96_digest_narrative"] }])
      expect(bsCode(() => validateBootstrapPlan(withSection, { ...bInput, ...change }))).toBe("release_bootstrap_approval");
    expect(bsCode(() => validateBootstrapPlan({ ...plan }, bInput))).toBe("release_plan_artifact");
    expect(bsCode(() => validateBootstrapPlan(withSection, { ...bInput, planRun: "91" }))).toBe("release_plan_artifact");
  }
  // Nothing held: the target generation is the full generation, and nothing else.
  const fullMatch = { ...plan, held: [], generation: planGeneration };
  expect(validateReleasePlan(fullMatch, { ...input, generation: planGeneration }).targetGeneration).toBe(planGeneration);
  for (const [p2, tg] of [
    [{ ...plan, held: [] }, planGeneration],
    [{ ...plan, generation: planGeneration }, planGeneration],
    [{ ...plan, targetGeneration: undefined }, planGeneration],
    [{ ...plan, targetGeneration: undefined }, ""],
    [{ ...plan, targetGeneration: planGeneration.toUpperCase() }, planGeneration.toUpperCase()],
    [{ ...plan, targetGeneration: "f".repeat(63) }, "f".repeat(63)],
    // A generation that is not the manifest's, even when the input agrees.
    [{ ...plan, targetGeneration: targetGeneration }, targetGeneration],
    [plan, "e".repeat(64)],
    [plan, ""],
  ] as const)
    expect(() => validateReleasePlan(p2, { ...input, targetGeneration: tg })).toThrow(
      "release_plan_artifact",
    );
  // Manifest binding: the plan's manifest, its digest and target_sha must agree with
  // each other and with the approval (plan-vs-apply drift of any piece refuses).
  const otherVerifiers = targetManifest.verifiers.map(([n], i) => [n, i === 4 ? "e".repeat(64) : targetManifest.verifiers[i]![1]] as [string, string]);
  const resealed = (m: typeof targetManifest) => ({
    ...plan,
    targetManifest: m,
    manifestDigest: releaseManifestDigest(m),
    targetGeneration: manifestGeneration(m),
  });
  for (const [p2, in2] of [
    [{ ...plan, targetManifest: undefined }, input],
    [{ ...plan, targetManifest: { ...targetManifest, extra: 1 } }, input],
    [{ ...plan, targetManifest: { ...targetManifest, verifiers: targetManifest.verifiers.slice(1) } }, input],
    [{ ...plan, targetManifest: { ...targetManifest, verifiers: [...targetManifest.verifiers].reverse() } }, input],
    [{ ...plan, targetManifest: { ...targetManifest, steps: [[prior, "1".repeat(63)], [target, "2".repeat(64)]] } }, input],
    [{ ...plan, targetManifest: { ...targetManifest, targetSha: targetSha.slice(0, 12) } }, input],
    // The manifest changed after the digest was computed (verifier or step drift).
    [{ ...plan, targetManifest: { ...targetManifest, verifiers: otherVerifiers } }, input],
    [{ ...plan, targetManifest: { ...targetManifest, steps: [[target, "2".repeat(64)]] } }, input],
    // Re-sealed with a consistent digest/generation, but not what was approved.
    [resealed({ ...targetManifest, verifiers: otherVerifiers }), input],
    // The approval's digest or target_sha differs from the plan's.
    [plan, { ...input, manifestDigest: "e".repeat(64) }],
    [plan, { ...input, targetSha: "7".repeat(40) }],
    [{ ...plan, targetSha: "7".repeat(40) }, input],
    [{ ...plan, manifestDigest: "e".repeat(64) }, { ...input, manifestDigest: "e".repeat(64) }],
    // A manifest for another target, or for another target commit, re-sealed.
    [resealed({ ...targetManifest, target: prior, steps: [[prior, "1".repeat(64)]] }), input],
    [resealed({ ...targetManifest, targetSha: "7".repeat(40) }), input],
    // A held step inside the manifest, or a pending step outside it.
    [{ ...plan, held: [prior] }, input],
    [{ ...plan, expectedPending: ["upstream:20261105045040_selfhost_output_facts.sql", target] }, input],
  ] as const)
    expect(() => validateReleasePlan(p2, in2)).toThrow("release_plan_artifact");
  for (const bad of [
    { target: "upstream:20261105046010_session_directory_read_api_query.sql" },
    { target: undefined },
    { expectedPending: undefined },
    { expectedPending: [] },
    { expectedPending: [target, prior] },
    { expectedPending: ["shim:supabase_roles", target] },
    { expectedPending: [target, 1] },
    { held: undefined },
    { held: "upstream:20261105050000_operator_session_erase.sql" },
    { held: ["verify:session_ingest"] },
  ])
    expect(() => validateReleasePlan({ ...plan, ...bad }, input)).toThrow(
      "release_plan_artifact",
    );
  for (const field of [
    "project",
    "sha",
    "generation",
    "boundary",
    "runId",
    "ledgerSha256",
    "targetGeneration",
    "targetSha",
    "manifestDigest",
    "targetManifest",
  ])
    expect(() =>
      validateReleasePlan({ ...plan, [field]: "wrong" }, input),
    ).toThrow();
  expect(() =>
    validateReleasePlan(
      { ...plan, existingBoundary: { ...existingBoundary, enabled_ok: false } },
      input,
    ),
  ).toThrow("release_boundary_identity");
});
test("workflow keeps credentials step-scoped and refuses implicit environment creation", () => {
  const w = readFileSync(
    new URL(
      "../../../.github/workflows/apply-session-schema.yml",
      import.meta.url,
    ),
    "utf8",
  );
  expect(w.indexOf("  prerequisite-gate:")).toBeLessThan(
    w.indexOf("    environment:"),
  );
  expect(w.match(/SUPABASE_MIGRATION_OWNER_URL:/g)?.length).toBe(1);
  expect(w).not.toMatch(
    /GITHUB_ENV|PGPASSWORD|1password|alchemy deploy|supabase db push/,
  );
  expect(w).toContain("cancel-in-progress: false");
  expect(w).toContain("persist-credentials: false");
  // The dispatch surface offers exactly one boundary and no event-trigger choice.
  const inputs = w.slice(w.indexOf("    inputs:\n"), w.indexOf("\npermissions:"));
  const boundary = inputs.slice(
    inputs.indexOf("      boundary:\n"),
    inputs.indexOf("      plan_run:\n"),
  );
  expect(boundary).toBe(
    [
      "      boundary:",
      "        description: Catalog-proven global control; apply refreshes the reviewed event trigger",
      "        type: choice",
      "        options: [global-default-revoke]",
      "        required: true",
      "",
    ].join("\n"),
  );
  expect(inputs).not.toContain("event-trigger");
  expect(w.match(/options: \[/g)).toEqual(["options: [", "options: ["]);
  expect(w).not.toContain("event-trigger");
  const parsed = Bun.YAML.parse(w) as any;
  expect(parsed.on.workflow_dispatch.inputs.boundary).toEqual({
    description:
      "Catalog-proven global control; apply refreshes the reviewed event trigger",
    type: "choice",
    options: ["global-default-revoke"],
    required: true,
  });
  expect(parsed.env.RELEASE_BOUNDARY).toBe("${{ inputs.boundary }}");
});
test("release boundary plan binding accepts only the exact safe trigger shape", () => {
  const boundary = {
    present: true,
    event: "ddl_command_end",
    tags_exact: true,
    enabled_ok: true,
    owner: P.owner,
    owner_is_migration_owner: true,
    function_is_reviewed: true,
    function_owner: P.owner,
    function_owner_is_migration_owner: true,
    schema_owner_is_migration_owner: true,
    prosrc_sha256: "a".repeat(64),
    proconfig_exact: true,
    security_definer: false,
    volatility: "v",
    language: "plpgsql",
    returns: "event_trigger",
    function_acl: ["<owner>:EXECUTE"],
    function_public_execute: false,
    runtime_role_setting_matches: true,
    schema_private: true,
  };
  expect(requireSafeReleaseBoundary(boundary)).toBe(boundary);
  expect(releaseBoundaryDigest(boundary)).toMatch(/^[0-9a-f]{64}$/);
  for (const unsafe of [
    { enabled_ok: false },
    { function_public_execute: true },
    { function_acl: ["<owner>:EXECUTE", "foreign:EXECUTE"] },
    { schema_private: false },
    { prosrc_sha256: "not-a-digest" },
  ])
    expect(() =>
      requireSafeReleaseBoundary({ ...boundary, ...unsafe }),
    ).toThrow("release_boundary_identity");
});
test("release boundary refresh refuses injected SQL before any database call", async () => {
  let calls = 0;
  await expect(
    refreshReleaseBoundary(
      { query: async () => (calls++, { rows: [] }) } as any,
      {},
      {
        sql: "SELECT 1",
        sha256: "a".repeat(64),
        prosrcSha256: "b".repeat(64),
      },
    ),
  ).rejects.toThrow("release_boundary_wrapper");
  expect(calls).toBe(0);
});
test("workflow gates on the exact repository id and downloads the plan from this repository", () => {
  const w = readFileSync(
    new URL(
      "../../../.github/workflows/apply-session-schema.yml",
      import.meta.url,
    ),
    "utf8",
  );
  const gates = [...w.matchAll(/^\s*if: (github\.repository.*)$/gm)].map((m) => m[1]);
  expect(gates).toEqual([
    "github.repository_id == '1437714333' && github.ref == 'refs/heads/main'",
  ]);
  // Unrelated repository ids or a name comparison must not satisfy the gate.
  const ids = [...w.matchAll(/github\.repository_id == '([^']*)'/g)].map((m) => m[1]);
  expect(ids).toEqual(["1437714333"]);
  for (const other of ["1437714334", "143771433", "252050005", ""]) expect(ids).not.toContain(other);
  expect(w).not.toMatch(/github\.repository ==|repository_id !=|repository_id >/);
  expect(w).not.toContain("sessions-pipeline");
  const plan = w.slice(w.indexOf("name: schema-release-plan"));
  expect(plan).toMatch(/^\s+repository: \$\{\{ github\.repository \}\}$/m);
});

test("connection enforces pinned CA/hostname and release profile ignores ambient bootstrap switches", async () => {
  const { releaseClient, releaseProfile } =
    await import("../schema-release-inspect.ts");
  const client = releaseClient(url);
  const ssl = client.connectionParameters.ssl as any;
  expect(ssl.rejectUnauthorized).toBe(true);
  expect(ssl.servername).toBe(P.host);
  expect(ssl.ca).toContain("BEGIN CERTIFICATE");
  const profile = releaseProfile("global-default-revoke");
  expect(profile).toEqual({
    mode: "supabase",
    shim: false,
    bootstrap: false,
    verify: true,
    runtimeRole: P.runtime,
    runtimeLogin: true,
    functionBoundary: "global-default-revoke",
  });
});
test("operator inspection starts READ ONLY and rolls back on wrong owner without schema SQL", async () => {
  const { inspectRelease } = await import("../schema-release-inspect.ts");
  const statements: string[] = [];
  const client = {
    query: async (sql: string) => {
      statements.push(sql);
      if (sql.includes("current_user AS me"))
        return {
          rows: [
            {
              me: "postgres",
              session: "postgres",
              db: "postgres",
              rolcanlogin: true,
            },
          ],
        };
      return { rows: [] };
    },
  };
  await expect(
    inspectRelease(client as any, "global-default-revoke"),
  ).rejects.toThrow("release_owner");
  expect(statements[0]).toBe("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  expect(statements.at(-1)).toBe("ROLLBACK");
  expect(
    statements.some((s) => /CREATE|ALTER|GRANT|INSERT|UPDATE|DELETE/.test(s)),
  ).toBe(false);
});
test("operator inspection accepts only the observed exact postgres owner shape", async () => {
  const { inspectRelease } = await import("../schema-release-inspect.ts");
  const statements: string[] = [];
  const client = {
    query: async (sql: string) => {
      statements.push(sql);
      if (sql.includes("current_user AS me"))
        return {
          rows: [
            {
              me: P.owner,
              session: P.owner,
              db: P.database,
              rolcanlogin: true,
              rolsuper: false,
              rolcreaterole: true,
              rolcreatedb: true,
              rolbypassrls: true,
              rolreplication: true,
              schema_owner: P.owner,
            },
          ],
        };
      if (sql.includes("pg_advisory_xact_lock")) throw new Error("after_identity");
      return { rows: [] };
    },
  };
  await expect(
    inspectRelease(client as any, "global-default-revoke"),
  ).rejects.toThrow("after_identity");
  expect(statements.at(-1)).toBe("ROLLBACK");
  expect(statements.some((s) => s.includes("pg_advisory_xact_lock"))).toBe(true);
});
test("operator plan refuses apply mode before reading a secret or connecting", () => {
  const r = Bun.spawnSync(
    [process.execPath, "scripts/schema-release-operator-plan.ts"],
    {
      cwd: new URL("../../../", import.meta.url).pathname,
      env: {
        ...process.env,
        ...base,
        RELEASE_MODE: "apply",
        RELEASE_PLAN_RUN: "90",
        RELEASE_TARGET_GENERATION: targetGeneration,
        RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
        SUPABASE_MIGRATION_OWNER_URL: "",
      },
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  expect(r.exitCode).toBe(1);
  expect(JSON.parse(new TextDecoder().decode(r.stdout))).toEqual({
    version: 1,
    status: "refused",
    code: "operator_plan_only",
  });
  expect(new TextDecoder().decode(r.stderr)).toBe("");
});
test("target is required, exact and bound into the typed approval", () => {
  for (const t of [
    undefined,
    "",
    "20261105047000",
    "upstream:20261105047000_coordination_fact_readers",
    "upstream:../20261105047000_x.sql",
    "verify:session_ingest",
    "96bulls:12345_x.sql",
    target + " ",
  ])
    expect(() => releaseInputs({ ...base, RELEASE_TARGET: t })).toThrow(
      "release_inputs",
    );
  expect(releaseInputs(base).target).toBe(target);
  expect(
    releaseInputs({ ...base, RELEASE_TARGET: "96bulls:0004_x.sql" }).target,
  ).toBe("96bulls:0004_x.sql");
  const apply = {
    ...base,
    RELEASE_MODE: "apply",
    RELEASE_PLAN_RUN: "90",
    RELEASE_TARGET_GENERATION: targetGeneration,
    RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
  };
  expect(releaseInputs(apply).target).toBe(target);
  const other = "upstream:20261105046010_session_directory_read_api_query.sql";
  for (const approval of [
    `APPLY ${P.project} ${sha} ${generation} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${other} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90 `,
    // The pre-targetGeneration format, and the full generation in its place.
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${generation} MANIFEST ${manifestDigest} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${"e".repeat(64)} MANIFEST ${manifestDigest} PLAN 90`,
    // The pre-anchoring format (no target_sha, no manifest), each piece missing, out
    // of order, abbreviated or malformed.
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetGeneration} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetGeneration} ${targetSha} MANIFEST ${manifestDigest} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha.slice(0, 12)} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${"7".repeat(40)} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest.slice(1)} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${"ab".repeat(32).toUpperCase()} PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST  PLAN 90`,
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} PLAN 90 MANIFEST ${manifestDigest}`,
  ])
    expect(() =>
      releaseInputs({ ...apply, RELEASE_APPROVAL: approval }),
    ).toThrow("release_typed_approval");
  // Approval for one target never covers a dispatch with another.
  expect(() => releaseInputs({ ...apply, RELEASE_TARGET: other })).toThrow(
    "release_typed_approval",
  );
});
test("target_sha is required in both modes, a full 40-hex lowercase commit id, and bound into the typed approval", () => {
  expect(releaseInputs(base).targetSha).toBe(targetSha);
  const apply = {
    ...base,
    RELEASE_MODE: "apply",
    RELEASE_PLAN_RUN: "90",
    RELEASE_TARGET_GENERATION: targetGeneration,
    RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
  };
  expect(releaseInputs(apply)).toMatchObject({ targetSha, manifestDigest });
  for (const ts of [undefined, "", targetSha.slice(0, 12), targetSha.slice(0, 39), targetSha + "0", "A".repeat(40), "HEAD", "main", `${targetSha} `, `-${targetSha.slice(1)}`])
    for (const env of [base, apply])
      expect(() => releaseInputs({ ...env, RELEASE_TARGET_SHA: ts })).toThrow("release_target_sha");
  // Approved for one target commit, dispatched with another.
  expect(() => releaseInputs({ ...apply, RELEASE_TARGET_SHA: "7".repeat(40) })).toThrow("release_typed_approval");
});
test("target generation is apply-only, exact 64 hex and bound into the typed approval", () => {
  const apply = {
    ...base,
    RELEASE_MODE: "apply",
    RELEASE_PLAN_RUN: "90",
    RELEASE_TARGET_GENERATION: targetGeneration,
    RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
  };
  expect(releaseInputs(apply).targetGeneration).toBe(targetGeneration);
  // Nothing held: the plan records targetGeneration = generation.
  expect(
    releaseInputs({
      ...apply,
      RELEASE_TARGET_GENERATION: generation,
      RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${generation} MANIFEST ${manifestDigest} PLAN 90`,
    }).targetGeneration,
  ).toBe(generation);
  // Approved for one target generation, dispatched with another (or none / malformed).
  for (const tg of [undefined, "", "e".repeat(64), generation, targetGeneration.toUpperCase(), "f".repeat(63), targetGeneration + " "]) {
    const env = { ...apply, RELEASE_TARGET_GENERATION: tg };
    expect(() => releaseInputs(env)).toThrow("release_typed_approval");
  }
  // A malformed value never validates even with a matching approval string.
  for (const tg of ["F".repeat(64), "f".repeat(65), "g".repeat(64)])
    expect(() =>
      releaseInputs({
        ...apply,
        RELEASE_TARGET_GENERATION: tg,
        RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${tg} MANIFEST ${manifestDigest} PLAN 90`,
      }),
    ).toThrow("release_typed_approval");
  // Plan mode refuses it (it is a plan output, never a plan input).
  expect(() => releaseInputs({ ...base, RELEASE_TARGET_GENERATION: targetGeneration })).toThrow(
    "release_plan_inputs",
  );
  expect(releaseInputs(base).targetGeneration).toBe("");
});
test("the GitHub gate refuses a missing or unapproved target before any API read", async () => {
  const noApi = async () => {
    throw new Error("api_read");
  };
  const apply = {
    ...base,
    RELEASE_MODE: "apply",
    RELEASE_PLAN_RUN: "90",
    RELEASE_TARGET_GENERATION: targetGeneration,
    RELEASE_APPROVAL: `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
  };
  for (const [env, code] of [
    [{ ...base, RELEASE_TARGET: undefined }, "release_inputs"],
    [{ ...base, RELEASE_TARGET: "upstream:x.sql" }, "release_inputs"],
    [
      { ...apply, RELEASE_TARGET: "upstream:20261105046010_x.sql" },
      "release_typed_approval",
    ],
  ] as const)
    await expect(githubReleaseGate(env, noApi, true)).rejects.toMatchObject({
      code,
    });
});
test("target split: exact pending prefix, held tail; unknown, applied and passed targets refuse", () => {
  const u = (v: string) => `upstream:2026110504${v}_s.sql`;
  const inv = (ledgered: string[]) =>
    [
      "shim:supabase_roles",
      u("5000"),
      u("6000"),
      u("7000"),
      u("7010"),
      u("8000"),
      "96bulls:0001_a.sql",
      "96bulls:0002_b.sql",
      "verify:session_ingest",
    ].map((name) => ({ name, pending: !ledgered.includes(name) }));
  const tail = [u("5000"), "96bulls:0001_a.sql", "96bulls:0002_b.sql"];
  expect(migrationTargetSplit(inv(tail), u("7010"))).toEqual({
    expectedPending: [u("6000"), u("7000"), u("7010")],
    held: [u("8000")],
  });
  // Target at the tree's tail: nothing held.
  expect(migrationTargetSplit(inv(tail), u("8000"))).toEqual({
    expectedPending: [u("6000"), u("7000"), u("7010"), u("8000")],
    held: [],
  });
  // An unledgered later lane is held too.
  expect(migrationTargetSplit(inv([u("5000")]), u("6000"))).toEqual({
    expectedPending: [u("6000")],
    held: [u("7000"), u("7010"), u("8000"), "96bulls:0001_a.sql", "96bulls:0002_b.sql"],
  });
  const code = (f: () => unknown) => {
    try {
      f();
    } catch (e) {
      return (e as { code?: string }).code;
    }
    return "no_refusal";
  };
  expect(code(() => migrationTargetSplit(inv(tail), u("9000")))).toBe("release_target_unknown");
  expect(code(() => migrationTargetSplit(inv(tail), "verify:session_ingest"))).toBe("release_target_unknown");
  expect(code(() => migrationTargetSplit(inv(tail), u("5000")))).toBe("release_target_not_pending");
  // Below the database tail of its own lane.
  expect(code(() => migrationTargetSplit(inv([...tail, u("7000")]), u("6000")))).toBe("release_target_passed");
  expect(
    code(() =>
      migrationTargetSplit(inv([u("5000"), "96bulls:0002_b.sql"]), "96bulls:0001_a.sql"),
    ),
  ).toBe("release_target_passed");
});
test("workflow carries the required target through env and into the approval text only", () => {
  const w = readFileSync(
    new URL("../../../.github/workflows/apply-session-schema.yml", import.meta.url),
    "utf8",
  );
  expect(w).toMatch(/\n      target:\n        description: "[^"\n]+"\n        type: string\n        required: true\n/);
  expect(w).toContain("  RELEASE_TARGET: ${{ inputs.target }}\n");
  expect(w).toContain(
    "<generation> TARGET <target> <target_sha> <target_generation> MANIFEST <manifest_digest> PLAN <plan_run>",
  );
  expect(w).toMatch(/\n      target_sha:\n        description: "[^"\n]+"\n        type: string\n        required: true\n/);
  expect(w).toContain("  RELEASE_TARGET_SHA: ${{ inputs.target_sha }}\n");
  // The database job needs full history: target_sha is read from the local object DB.
  expect(w).toMatch(/ref: \$\{\{ github\.sha \}\}\n\s+fetch-depth: 0\n/);
  expect(w).toMatch(/\n      target_generation:\n        description: "[^"\n]+"\n        type: string\n        default: ""\n/);
  expect(w).toContain("  RELEASE_TARGET_GENERATION: ${{ inputs.target_generation }}\n");
  // Only the env block interpolates the inputs; no run: script does.
  expect(w.match(/inputs\.target\b/g)).toHaveLength(1);
  expect(w.match(/inputs\.target_generation\b/g)).toHaveLength(1);
  expect(w.match(/inputs\.target_sha\b/g)).toHaveLength(1);
});
test("operator plan renders the target split and refuses a missing target before connecting", () => {
  const src = readFileSync(
    new URL("../../../scripts/schema-release-operator-plan.ts", import.meta.url),
    "utf8",
  );
  expect(src).toContain("migrationTargetSplit(report.inventory, input.target)");
  expect(src).toMatch(
    /target: input\.target,\n\s+\.\.\.split,\n\s+targetSha: anchor\.targetSha,\n\s+targetGeneration,\n\s+manifestDigest: anchor\.manifestDigest,\n\s+targetManifest: anchor\.manifest,/,
  );
  expect(src).toContain("const anchor = releaseTargetBinding({");
  expect(src).toContain("prefix: await schemaStepDigests(split.held),");
  const r = Bun.spawnSync(
    [process.execPath, "scripts/schema-release-operator-plan.ts"],
    {
      cwd: new URL("../../../", import.meta.url).pathname,
      env: { ...process.env, ...base, RELEASE_TARGET: "", SUPABASE_MIGRATION_OWNER_URL: "" },
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  expect(r.exitCode).toBe(1);
  expect(JSON.parse(new TextDecoder().decode(r.stdout))).toEqual({
    version: 1,
    status: "refused",
    code: "release_inputs",
  });
});
test("release apply binds the plan's target generation into the migrator and the PASS readback", () => {
  const src = readFileSync(
    new URL("../../../scripts/schema-release-database.ts", import.meta.url),
    "utf8",
  );
  // Plan records target_sha's anchor; apply recomputes it before connecting, from the
  // live split, and once more under the migrator lock, and refuses any drift.
  expect(src).toContain("const anchor = await targetAnchor(input, split.held);");
  expect(src).toMatch(
    /\.\.\.split,\n\s+targetSha: anchor\.targetSha,\n\s+targetGeneration,\n\s+manifestDigest: anchor\.manifestDigest,\n\s+targetManifest: anchor\.manifest,\n\s+runId/,
  );
  expect(src).toMatch(/approvedPlan &&\n\s+!sameAnchor\(await targetAnchor\(input, approvedPlan\.held\), approvedPlan\)\n\s+\)\n\s+refuse\("release_target_manifest_changed"\);/);
  expect(src).toContain('if (!sameAnchor(anchor, approvedPlan!)) refuse("release_target_manifest_changed");');
  expect(src).toMatch(
    /beforeWrite: async \(\) => \{\n\s+await githubReleaseGate\(process\.env, get, true\);\n(\s+\/\/[^\n]*\n)*\s+const fresh = await targetAnchor\(input, approvedPlan!\.held\);\n\s+if \(!sameAnchor\(fresh, approvedPlan!\)\) refuse\("release_target_manifest_changed"\);/,
  );
  expect(src).toMatch(
    /targetBinding: \{\n(\s+\/\/[^\n]*\n)*\s+targetSha: approvedPlan!\.targetSha,\n\s+manifestDigest: approvedPlan!\.manifestDigest,\n\s+verifiers: anchor\.verifiers,/,
  );
  expect(src).toMatch(/if \(targetGeneration !== approvedPlan!\.targetGeneration\)\n\s+refuse\("release_target_generation_changed"\);/);
  expect(src).toContain("targetGeneration: approvedPlan!.targetGeneration,");
  // PASS for exactly the target generation, never a 'pending' acceptance.
  expect(src).toMatch(/marker\?\.status !== "pass" \|\|\n\s+marker\.generation !== approvedPlan!\.targetGeneration/);
  expect(src).not.toContain('"pending"');
});
test("operator plan refuses an event-trigger boundary as release_inputs before reading a secret or connecting", () => {
  const r = Bun.spawnSync(
    [process.execPath, "scripts/schema-release-operator-plan.ts"],
    {
      cwd: new URL("../../../", import.meta.url).pathname,
      env: {
        ...process.env,
        ...base,
        RELEASE_BOUNDARY: "event-trigger",
        // Never a connectable URL here: a regression must not reach the network.
        SUPABASE_MIGRATION_OWNER_URL: "",
      },
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  expect(r.exitCode).toBe(1);
  expect(JSON.parse(new TextDecoder().decode(r.stdout))).toEqual({
    version: 1,
    status: "refused",
    code: "release_inputs",
  });
  expect(new TextDecoder().decode(r.stderr)).toBe("");
});

// ---- Bootstrap-role stage (src/hoopdb/schema-release-bootstrap.ts): pure verdicts ----
const bsRole = (rolname: string, o: Record<string, unknown> = {}) => ({
  rolname,
  rolsuper: false,
  rolinherit: false,
  rolcreaterole: false,
  rolcreatedb: false,
  rolcanlogin: false,
  rolreplication: false,
  rolbypassrls: false,
  rolconnlimit: -1,
  rolvaliduntil: null,
  ...o,
});
const creatorEdge = (role: string, o: Partial<BootstrapEdge> = {}): BootstrapEdge => ({
  role,
  member: "postgres",
  member_inherit: true,
  grantor: "supabase_admin",
  grantor_super: true,
  admin_option: true,
  inherit_option: false,
  set_option: false,
  ...o,
});
const chainNames = bootstrapChainRoles().map((r) => r.name);
const workerSetting = {
  role: "session_directory_search_worker",
  global: true,
  kind: "statement_timeout=2s" as const,
  database: "0",
  digest: "d".repeat(64),
};
/** A PG16 Supabase cluster where every chain role exists exactly, minus `absent`. */
function bsState(absent: string[] = [], o: Partial<BootstrapState> = {}): BootstrapState {
  return {
    serverVersionNum: 170004,
    roles: [
      bsRole("postgres", { rolcreaterole: true, rolcanlogin: true, rolbypassrls: true }),
      bsRole("supabase_admin", { rolsuper: true, rolcanlogin: true }),
      bsRole("authenticator", { rolcanlogin: true }),
      ...chainNames
        .filter((n) => !absent.includes(n))
        .map((n) => bsRole(n, n === "bulls96_read_api" ? { rolcanlogin: true } : {})),
    ],
    edges: [],
    settings: absent.includes("session_directory_search_worker")
      ? []
      : [workerSetting],
    ...o,
  };
}
const bsRepo = new URL("../../../", import.meta.url);
const bsBytes = (file: string) => readFileSync(new URL(file, bsRepo), "utf8");
/** HEAD chain bytes as both the anchor's (target) and the release sha's; overrides per side. */
const bsFiles = (
  release: Record<string, string | undefined> = {},
  anchor: Record<string, string | undefined> = {},
) =>
  SESSION_BOOTSTRAP_FILES.map((file) => ({
    file,
    release: file in release ? release[file] : bsBytes(file),
    target: file in anchor ? anchor[file] : bsBytes(file),
  }));
/** The anchor's contract modules: HEAD's (contract_source "anchor"). */
const bsContract = {
  rolesModule: bsBytes("src/hoopdb/session-bootstrap-roles.ts"),
  consumersModule: bsBytes("src/hoopdb/session-bootstrap-consumers.ts"),
};
/** The anchor's ordered schema steps: the HEAD upstream migrations (fixture). */
const bsSteps = readdirSync(new URL("supabase/migrations/", bsRepo))
  .filter((f) => f.endsWith(".sql"))
  .sort()
  .map((f) => `upstream:${f}`);
const bsSection = (x: {
  files?: ReturnType<typeof bsFiles>;
  state?: BootstrapState;
  supabaseMode?: boolean;
  log?: (line: string) => void;
  target?: string;
}) =>
  bootstrapSection({
    target: x.target ?? target,
    targetSha,
    steps: bsSteps,
    files: x.files ?? bsFiles(),
    contract: bsContract,
    state: x.state ?? bsState(),
    supabaseMode: x.supabaseMode ?? true,
    log: x.log,
  });
const bsCode = (f: () => unknown) => {
  try {
    f();
  } catch (e) {
    return (e as { code?: string }).code;
  }
  return "no-refusal";
};
const quiet = () => {};
const admit = "bulls96_analysis_admit";
const readApiFile = "supabase/bootstrap/session_read_api_roles.sql";

test("bootstrap chain_digest: sha256 over target, target_sha, the required set, the contract digest and the run files' anchor digests", () => {
  const files = SESSION_BOOTSTRAP_FILES.map((file, i) => ({
    file,
    sha256_target: String(i).repeat(64),
    scope: (i === 2 ? "excluded" : "run") as "run" | "excluded",
  }));
  const contract_digest = "c".repeat(64);
  const x = { target, target_sha: targetSha, required_roles: ["b_role", "a_role"], contract_digest, files };
  const expected = createHash("sha256")
    .update(
      `target ${target}\ntarget_sha ${targetSha}\nrequired a_role,b_role\ncontract ${contract_digest}\n` +
        files
          .filter((f) => f.scope === "run")
          .map((f) => `${f.file}\n${f.sha256_target}\n`)
          .join(""),
    )
    .digest("hex");
  expect(bootstrapChainDigest(x)).toBe(expected);
  expect(bootstrapChainDigest({ ...x, files: [...files].reverse() })).not.toBe(expected);
  expect(bootstrapChainDigest({ ...x, required_roles: ["a_role"] })).not.toBe(expected);
  expect(bootstrapChainDigest({ ...x, target_sha: "7".repeat(40) })).not.toBe(expected);
  expect(bootstrapChainDigest({ ...x, target: "upstream:20261105046000_analysis_jobs.sql" })).not.toBe(expected);
  expect(bootstrapChainDigest({ ...x, contract_digest: "d".repeat(64) })).not.toBe(expected);
  // An excluded file's digest is not part of CHAIN.
  expect(bootstrapChainDigest({ ...x, files: files.map((f, i) => (i === 2 ? { ...f, sha256_target: null } : f)) })).toBe(expected);
  expect(
    createHash("sha256").update(`target ${target}\ntarget_sha ${targetSha}\nrequired none\ncontract ${contract_digest}\n`).digest("hex"),
  ).toBe(bootstrapChainDigest({ ...x, required_roles: [], files: [] }));
  const section = bsSection({});
  expect(section.chain_digest).toBe(bootstrapChainDigest(section));
  expect(section.contract_digest).toBe(bootstrapContractDigest());
  // The anchor's bytes bind CHAIN; the release sha's bytes only set `differs`.
  expect(bsSection({ files: bsFiles({}, { [readApiFile]: bsBytes(readApiFile) + "\n-- other\n" }) }).chain_digest).not.toBe(
    section.chain_digest,
  );
  const drift = bsSection({ files: bsFiles({ [readApiFile]: "-- other bytes\n" }) });
  expect(drift.chain_digest).toBe(section.chain_digest);
  expect(drift.release_drift).toEqual([readApiFile]);
});

test("bootstrap typed approval: exact canonical string, sorted roles or none", () => {
  const x = {
    project: P.project,
    sha,
    target,
    targetSha,
    chainDigest: "c".repeat(64),
    planRun: "90",
  };
  expect(bootstrapApproval({ ...x, willCreate: ["b_role", "a_role"] })).toBe(
    `BOOTSTRAP ${P.project} ${sha} TARGET ${target} ${targetSha} CHAIN ${"c".repeat(64)} ROLES a_role,b_role PLAN 90`,
  );
  expect(bootstrapApproval({ ...x, willCreate: [] })).toBe(
    `BOOTSTRAP ${P.project} ${sha} TARGET ${target} ${targetSha} CHAIN ${"c".repeat(64)} ROLES none PLAN 90`,
  );
  const env = {
    ...base,
    RELEASE_MODE: "bootstrap",
    RELEASE_PLAN_RUN: "90",
    RELEASE_TARGET_GENERATION: targetGeneration,
    RELEASE_APPROVAL: bootstrapApproval({ ...x, willCreate: [admit] }),
  };
  const ok = releaseInputs(env);
  expect(ok.mode).toBe("bootstrap");
  expect(ok.chainDigest).toBe("c".repeat(64));
  expect(ok.bootstrapRoles).toEqual([admit]);
  expect(releaseInputs({ ...env, RELEASE_APPROVAL: bootstrapApproval({ ...x, willCreate: [] }) }).bootstrapRoles).toEqual([]);
  const good = env.RELEASE_APPROVAL;
  for (const approval of [
    good.replace(P.project, "anotherprojectrefxxxx"),
    good.replace(`BOOTSTRAP ${P.project}`, "BOOTSTRAP <project>"),
    good.replace(" PLAN 90", " PLAN 91"),
    good.replace(`ROLES ${admit}`, `ROLES ${admit},${admit}`),
    good.replace(`ROLES ${admit}`, `ROLES zz_role,${admit}`),
    good.replace(`ROLES ${admit}`, "ROLES "),
    good.replace(`ROLES ${admit}`, `ROLES ${admit.toUpperCase()}`),
    good.replace("CHAIN ", "CHAIN  "),
    good.replace(targetSha, "7".repeat(40)),
    good + " ",
    `APPLY ${P.project} ${sha} ${generation} TARGET ${target} ${targetSha} ${targetGeneration} MANIFEST ${manifestDigest} PLAN 90`,
    "",
  ])
    expect(bsCode(() => releaseInputs({ ...env, RELEASE_APPROVAL: approval }))).toBe(
      "release_bootstrap_approval",
    );
  expect(bsCode(() => releaseInputs({ ...env, RELEASE_PLAN_RUN: "" }))).toBe("release_bootstrap_approval");
  expect(bsCode(() => releaseInputs({ ...env, RELEASE_TARGET_GENERATION: "" }))).toBe("release_bootstrap_approval");
  // A BOOTSTRAP string never authorizes apply, and other modes stay refused.
  expect(bsCode(() => releaseInputs({ ...env, RELEASE_MODE: "apply" }))).toBe("release_typed_approval");
  expect(bsCode(() => releaseInputs({ ...env, RELEASE_MODE: "Bootstrap" }))).toBe("release_inputs");
});

test("bootstrap plan section shape: files, roles, required, will_create, next", () => {
  const present = bsSection({ files: bsFiles(), state: bsState(), supabaseMode: true });
  expect(Object.keys(present)).toEqual([
    "db_mode",
    "target",
    "target_sha",
    "files",
    "roles",
    "required_roles",
    "excluded",
    "mixed",
    "release_drift",
    "required",
    "will_create",
    "contract_source",
    "contract_map_digest",
    "contract_manifest_digest",
    "contract_digest",
    "chain_digest",
    "next",
  ]);
  expect(present.target).toBe(target);
  expect(present.target_sha).toBe(targetSha);
  expect(present.required_roles).toEqual([...chainNames].sort());
  expect([present.excluded, present.mixed, present.release_drift]).toEqual([[], [], []]);
  expect(present.db_mode).toBe("supabase");
  expect(present.required).toBe(false);
  expect(present.will_create).toEqual([]);
  expect(present.next).toBe("apply");
  expect(present.files.map((f) => f.file)).toEqual([...SESSION_BOOTSTRAP_FILES]);
  expect(Object.keys(present.files[0])).toEqual(["file", "sha256_target", "sha256_release", "differs", "scope"]);
  expect(present.files.every((f) => f.scope === "run")).toBe(true);
  expect(present.files.every((f) => !f.differs)).toBe(true);
  expect(present.roles.map((r) => r.name)).toEqual(chainNames);
  expect(Object.keys(present.roles[0])).toEqual(["name", "file", "first_consumer", "required", "present", "exact", "will_create"]);
  expect(present.roles.every((r) => r.present && r.exact && !r.will_create)).toBe(true);
  expect(validateBootstrapSection(JSON.parse(JSON.stringify(present)))).toEqual(present);

  const needed = bsSection({ files: bsFiles(), state: bsState([admit]), supabaseMode: true });
  expect(needed.required).toBe(true);
  expect(needed.will_create).toEqual([admit]);
  expect(needed.next).toBe("bootstrap stage required before apply");
  expect(needed.roles.find((r) => r.name === admit)).toEqual({
    name: admit,
    file: readApiFile,
    first_consumer: "20261105046000_analysis_jobs.sql",
    required: true,
    present: false,
    exact: false,
    will_create: true,
  });
  expect(bootstrapFilesToRun(needed)).toEqual([readApiFile]);
  expect(bootstrapFilesToRun(present)).toEqual([]);
  expect(validateBootstrapSection(JSON.parse(JSON.stringify(needed)))).toEqual(needed);
  for (const tamper of [
    (v: any) => (v.required = false),
    (v: any) => (v.will_create = []),
    (v: any) => (v.chain_digest = "0".repeat(64)),
    (v: any) => (v.next = "apply"),
    (v: any) => (v.db_mode = "rds"),
    (v: any) => v.roles.reverse(),
    (v: any) => (v.roles[0].exact = false),
    (v: any) => (v.files[0].differs = true),
    (v: any) => v.files.pop(),
    (v: any) => (v.will_create = [admit, "x_role"]),
    (v: any) => (v.target = "upstream:20261105046000_analysis_jobs.sql"),
    (v: any) => (v.target = "nope"),
    (v: any) => (v.target_sha = "A".repeat(40)),
    (v: any) => (v.required_roles = v.required_roles.slice(1)),
    (v: any) => (v.excluded = [readApiFile]),
    (v: any) => (v.files[3].scope = "excluded"),
    (v: any) => (v.mixed = [readApiFile]),
    (v: any) => (v.release_drift = [readApiFile]),
    (v: any) => (v.roles[0].required = false),
    (v: any) => (v.roles[0].first_consumer = "20261105047000_coordination_fact_readers.sql"),
    (v: any) => (v.roles[0].exact = true, v.roles[0].present = false),
    (v: any) => (v.contract_source = "plan"),
    (v: any) => delete v.contract_source,
    (v: any) => (v.contract_map_digest = "0".repeat(64)),
    (v: any) => (v.contract_manifest_digest = "0".repeat(64)),
    (v: any) => (v.contract_digest = "0".repeat(64)),
  ]) {
    const v = JSON.parse(JSON.stringify(needed));
    tamper(v);
    expect(bsCode(() => validateBootstrapSection(v))).toBe("release_plan_artifact");
  }
  expect(bsCode(() => validateBootstrapSection(undefined))).toBe("release_plan_artifact");

  // A lower target: required set recomputed from the target alone, excluded/mixed lists self-consistent.
  const low = bsSection({ target: "upstream:20261105033000_canonical_project_events.sql" });
  expect(validateBootstrapSection(JSON.parse(JSON.stringify(low)))).toEqual(low);
  expect(low.required_roles).toEqual(targetRequiredRoles(low.target));
  expect(low.excluded).toEqual([readApiFile, "supabase/bootstrap/session_consumer_roles.sql"]);
  expect(low.mixed).toEqual(["supabase/bootstrap/session_ingest_roles.sql"]);

  // Release-sha digests are informational: a differing or absent release file is reported.
  const info = bsSection({ files: bsFiles({ [SESSION_BOOTSTRAP_FILES[1]]: "-- release differs\n", [SESSION_BOOTSTRAP_FILES[2]]: undefined }) });
  expect(info.files[1].differs).toBe(true);
  expect(info.files[2]).toMatchObject({ sha256_release: null, differs: true });
  expect(info.files[0].differs).toBe(false);
  expect(info.release_drift).toEqual([SESSION_BOOTSTRAP_FILES[1], SESSION_BOOTSTRAP_FILES[2]]);
  expect(info.chain_digest).toBe(present.chain_digest);
  expect(validateBootstrapSection(JSON.parse(JSON.stringify(info)))).toEqual(info);
  // Anchor bytes absent for a required file; files out of chain order; an anchor file
  // provisioning an undeclared role, or missing a required declared one.
  expect(bsCode(() => bsSection({ files: bsFiles({}, { [readApiFile]: undefined }) }))).toBe("release_bootstrap_bytes_missing");
  expect(bsCode(() => bsSection({ files: bsFiles().reverse() }))).toBe("release_bootstrap_chain");
  expect(bsCode(() => bsSection({ files: bsFiles({}, { [readApiFile]: bsBytes(readApiFile) + "\nCREATE ROLE rogue_role;\n" }) }))).toBe(
    "release_bootstrap_chain",
  );
  expect(
    bsCode(() => bsSection({ files: bsFiles({}, { [readApiFile]: bsBytes(readApiFile).replaceAll("bulls96_analysis_admit", "bulls96_other_role") }) })),
  ).toBe("release_bootstrap_chain");
});

test("bootstrap exactness: inbound, outbound and flags follow the bulls96_read_api block", () => {
  const readApi = SESSION_BOOTSTRAP_ROLES[readApiFile].bulls96_read_api;
  const drift = (state: BootstrapState, supabaseMode = true) =>
    bsCode(() => bsSection({ files: bsFiles(), state, supabaseMode, log: quiet }));
  // PG15: any inbound member refuses (no inherit/set columns to prove a creator edge).
  const pg15 = bsState([], {
    serverVersionNum: 150008,
    edges: [creatorEdge("bulls96_read_api", { inherit_option: null, set_option: null })],
  });
  expect(roleContractProblems("bulls96_read_api", readApi, pg15, true)).toEqual(["member postgres"]);
  expect(drift(pg15)).toBe("release_bootstrap_role_drift");
  // PG16+: a non-creator edge refuses.
  for (const edge of [
    creatorEdge("bulls96_read_api", { member: "authenticator" }),
    creatorEdge("bulls96_read_api", { grantor: "postgres", grantor_super: false }),
    creatorEdge("bulls96_read_api", { grantor_super: false }),
    creatorEdge("bulls96_read_api", { admin_option: false }),
    creatorEdge("bulls96_read_api", { inherit_option: true }),
    creatorEdge("bulls96_read_api", { set_option: true }),
  ])
    expect(drift(bsState([], { edges: [edge] }))).toBe("release_bootstrap_role_drift");
  // The creator edge is accepted only in supabase db mode.
  const creator = bsState([], { edges: [creatorEdge("bulls96_read_api")] });
  expect(roleContractProblems("bulls96_read_api", readApi, creator, true)).toEqual([]);
  expect(roleContractProblems("bulls96_read_api", readApi, creator, false)).toEqual(["member postgres"]);
  expect(bsSection({ files: bsFiles(), state: creator, supabaseMode: true }).required).toBe(false);
  expect(drift(creator, false)).toBe("release_bootstrap_role_drift");
  // Outbound membership refuses for every chain role, even an inbound-unchecked one.
  for (const name of ["bulls96_read_api", admit, "bulls96_session_ingest"])
    expect(drift(bsState([], { edges: [{ ...creatorEdge("pg_read_all_data"), member: name }] }))).toBe(
      "release_bootstrap_role_drift",
    );
  // Wrong flags refuse; the drifted names go to stderr only.
  for (const [name, flags] of [
    ["bulls96_read_api", { rolinherit: true }],
    ["bulls96_read_api", { rolbypassrls: true }],
    [admit, { rolsuper: true }],
    [admit, { rolcreaterole: true }],
    ["bulls96_session_ingest", { rolcreatedb: true }],
    ["bulls96_session_ingest", { rolreplication: true }],
    ["bulls96_digest_narrative", { rolconnlimit: 5 }],
    ["bulls96_session_ingest_seeder", { rolcanlogin: true }],
    ["session_token_verifier", { rolcanlogin: true }],
  ] as const) {
    const state = bsState();
    state.roles = state.roles.map((r) => (r.rolname === name ? { ...r, ...flags } : r));
    const lines: string[] = [];
    expect(bsCode(() => bsSection({ files: bsFiles(), state, supabaseMode: true, log: (l) => lines.push(l) }))).toBe(
      "release_bootstrap_role_drift",
    );
    expect(lines).toEqual([`release_bootstrap_role_drift ${name}`]);
  }
  // A LOGIN-capable role (prod's bulls96_read_api) stays exact.
  expect(roleContractProblems("bulls96_read_api", readApi, bsState(), true)).toEqual([]);
  // Worker: exactly one global statement_timeout=2s; authenticator only NOINHERIT, no admin.
  const worker = "session_directory_search_worker";
  const wc = SESSION_BOOTSTRAP_ROLES["supabase/bootstrap/session_directory_search_worker.sql"][worker];
  expect(roleContractProblems(worker, wc, bsState([], { settings: [] }), true)).toEqual(["settings"]);
  expect(roleContractProblems(worker, wc, bsState([], { settings: [{ role: worker, global: false, kind: "statement_timeout=2s" }] }), true)).toEqual(["settings"]);
  const auth = { ...creatorEdge(worker), member: "authenticator", member_inherit: false, admin_option: false, inherit_option: false, set_option: true };
  expect(roleContractProblems(worker, wc, bsState([], { edges: [auth] }), true)).toEqual([]);
  expect(roleContractProblems(worker, wc, bsState([], { edges: [{ ...auth, member_inherit: true }] }), true)).toEqual(["member authenticator"]);
  // Consumer: bare, or LOGIN with exactly one global statement_timeout=60s.
  const consumer = "search_consumer_runtime";
  const cc = SESSION_BOOTSTRAP_ROLES["supabase/bootstrap/session_consumer_roles.sql"][consumer];
  const login = (s: BootstrapState) => ({ ...s, roles: s.roles.map((r) => (r.rolname === consumer ? { ...r, rolcanlogin: true } : r)) });
  expect(roleContractProblems(consumer, cc, login(bsState()), true)).toEqual(["settings"]);
  const sixty = bsState();
  sixty.settings = [...sixty.settings, { role: consumer, global: true, kind: "statement_timeout=60s" }];
  expect(roleContractProblems(consumer, cc, login(sixty), true)).toEqual([]);
  expect(roleContractProblems(consumer, cc, sixty, true)).toEqual(["settings"]);
});

test("bootstrap plan_changed: files, chain_digest, required or will_create drift refuses", () => {
  const planned = bsSection({ files: bsFiles(), state: bsState([admit]), supabaseMode: true });
  expect(() => requireSameBootstrapPlan(planned, structuredClone(planned))).not.toThrow();
  const other = bsSection({ files: bsFiles({}, { [readApiFile]: bsBytes(readApiFile) + "\n-- moved\n" }), state: bsState([admit]), supabaseMode: true });
  expect(bsCode(() => requireSameBootstrapPlan(planned, other))).toBe("release_bootstrap_plan_changed");
  const created = bsSection({ files: bsFiles(), state: bsState(), supabaseMode: true });
  expect(bsCode(() => requireSameBootstrapPlan(planned, created))).toBe("release_bootstrap_plan_changed");
  const two = bsSection({ files: bsFiles(), state: bsState([admit, "bulls96_digest_narrative"]), supabaseMode: true });
  expect(bsCode(() => requireSameBootstrapPlan(planned, two))).toBe("release_bootstrap_plan_changed");
  expect(bsCode(() => requireSameBootstrapPlan(planned, { ...planned, required_roles: [] }))).toBe("release_bootstrap_plan_changed");
  expect(bsCode(() => requireSameBootstrapPlan(planned, { ...planned, target_sha: "7".repeat(40) }))).toBe("release_bootstrap_plan_changed");
  expect(bsCode(() => requireSameBootstrapPlan(planned, { ...planned, excluded: [readApiFile] }))).toBe("release_bootstrap_plan_changed");
  for (const k of ["chain_digest", "required"] as const)
    expect(bsCode(() => requireSameBootstrapPlan(planned, { ...planned, [k]: k === "required" ? false : "0".repeat(64) }))).toBe(
      "release_bootstrap_plan_changed",
    );
});

test("bootstrap readback: exactly the approved roles appear; extra, missing or altered refuses", () => {
  const before = bsState([admit]);
  const created = (o: Partial<BootstrapState> = {}) =>
    bsState([], { edges: [creatorEdge(admit)], ...o });
  const ev = bootstrapReadbackVerdict({ before, after: created(), approved: [admit], supabaseMode: true });
  expect(ev.created).toEqual([admit]);
  expect(ev.problems).toEqual([]);
  expect(ev.created_edges).toEqual([
    { role: admit, member: "postgres", grantor: "supabase_admin", admin_option: true, inherit_option: false, set_option: false },
  ]);
  expect(ev.roles_after.length).toBe(chainNames.length);
  expect(JSON.stringify(ev)).not.toMatch(/password|rolconfig|setconfig|pooler|postgres(ql)?:\/\//);
  const refuseOn = (after: BootstrapState, approved = [admit], b = before) =>
    bsCode(() => {
      const err = console.error;
      console.error = quiet;
      try {
        bootstrapReadbackVerdict({ before: b, after, approved, supabaseMode: true });
      } finally {
        console.error = err;
      }
    });
  // Extra: another role appeared.
  const extra = created();
  extra.roles = [...extra.roles, bsRole("rogue_role")];
  expect(refuseOn(extra)).toBe("release_bootstrap_readback");
  // Missing: the approved role did not appear.
  expect(refuseOn(bsState([admit]))).toBe("release_bootstrap_readback");
  // Created more than approved.
  expect(refuseOn(created(), [], bsState([admit, "bulls96_digest_narrative"]))).toBe("release_bootstrap_readback");
  // Altered: an unrelated role's flags or a chain role's flags changed.
  for (const name of ["authenticator", "bulls96_read_api"]) {
    const altered = created();
    altered.roles = altered.roles.map((r) => (r.rolname === name ? { ...r, rolcanlogin: !r.rolcanlogin } : r));
    expect(refuseOn(altered)).toBe("release_bootstrap_readback");
  }
  // Removed: a role disappeared.
  const removed = created();
  removed.roles = removed.roles.filter((r) => r.rolname !== "authenticator");
  expect(refuseOn(removed)).toBe("release_bootstrap_readback");
  // The created role is not the bare shape: LOGIN, extra member, a setting, PG15 edge.
  const loginRole = created();
  loginRole.roles = loginRole.roles.map((r) => (r.rolname === admit ? { ...r, rolcanlogin: true } : r));
  expect(refuseOn(loginRole)).toBe("release_bootstrap_readback");
  expect(refuseOn(created({ edges: [creatorEdge(admit), creatorEdge(admit, { member: "authenticator" })] }))).toBe("release_bootstrap_readback");
  expect(refuseOn(created({ settings: [...before.settings, { role: admit, global: true, kind: "other" }] }))).toBe("release_bootstrap_readback");
  const err = console.error;
  console.error = quiet;
  try {
    expect(
      bsCode(() =>
        bootstrapReadbackVerdict({ before, after: created(), approved: [admit], supabaseMode: false }),
      ),
    ).toBe("release_bootstrap_readback");
  } finally {
    console.error = err;
  }
  // A pre-existing chain role's edges or settings changed.
  expect(refuseOn(created({ edges: [creatorEdge(admit), creatorEdge("bulls96_read_api")] }))).toBe("release_bootstrap_readback");
  expect(refuseOn(created({ settings: [] }))).toBe("release_bootstrap_readback");
});

test("bootstrap readback covers every role's validity, every membership edge and every setting row", () => {
  // A cluster with unrelated edges and settings that the stage must leave untouched.
  const otherEdge: BootstrapEdge = { role: "pg_read_all_data", member: "authenticator", member_inherit: false, grantor: "supabase_admin", grantor_super: true, admin_option: false, inherit_option: true, set_option: true };
  const dbWide = { role: "", global: false, kind: "other" as const, database: "5", digest: "a".repeat(64) };
  const authSetting = { role: "authenticator", global: true, kind: "other" as const, database: "0", digest: "b".repeat(64) };
  const cluster = (o: Partial<BootstrapState> = {}, absent: string[] = []) =>
    bsState(absent, { edges: [otherEdge], settings: [workerSetting, dbWide, authSetting], ...o });
  const before = cluster({}, [admit]);
  const after = (o: Partial<BootstrapState> = {}) =>
    cluster({ edges: [otherEdge, creatorEdge(admit)], ...o });
  const verdict = (a: BootstrapState, supabaseMode = true) =>
    bsCode(() => {
      const err = console.error;
      console.error = quiet;
      try {
        bootstrapReadbackVerdict({ before, after: a, approved: [admit], supabaseMode });
      } finally {
        console.error = err;
      }
    });
  expect(verdict(after())).toBe("no-refusal");
  const ev = bootstrapReadbackVerdict({ before, after: after(), approved: [admit], supabaseMode: true });
  // Setting digests are compared only; never written to evidence.
  expect(JSON.stringify(ev)).not.toContain("a".repeat(64));
  expect(JSON.stringify(ev)).not.toContain("b".repeat(64));
  expect(JSON.stringify(ev)).not.toContain("d".repeat(64));
  // Any non-chain edge added, removed or with a changed grantor/option refuses.
  for (const edges of [
    [creatorEdge(admit)],
    [otherEdge, creatorEdge(admit), { ...otherEdge, member: "postgres" }],
    [{ ...otherEdge, admin_option: true }, creatorEdge(admit)],
    [{ ...otherEdge, inherit_option: false }, creatorEdge(admit)],
    [{ ...otherEdge, set_option: false }, creatorEdge(admit)],
    [{ ...otherEdge, grantor: "postgres", grantor_super: false }, creatorEdge(admit)],
    // A created role gaining an edge other than its creator edge, or as a member.
    [otherEdge, creatorEdge(admit), creatorEdge(admit, { member: "authenticator", admin_option: false })],
    [otherEdge, creatorEdge(admit), { ...otherEdge, member: admit }],
  ])
    expect(verdict(after({ edges }))).toBe("release_bootstrap_readback");
  // The creator edge is permitted only in supabase db mode on PG16+.
  expect(verdict(after(), false)).toBe("release_bootstrap_readback");
  // Any setting row changed, added or removed, database-wide rows included.
  for (const settings of [
    [workerSetting, authSetting],
    [workerSetting, dbWide, { ...authSetting, digest: "c".repeat(64) }],
    [workerSetting, dbWide, { ...authSetting, database: "5", global: false }],
    [workerSetting, dbWide, authSetting, { ...dbWide, database: "6" }],
    [workerSetting, dbWide, authSetting, { ...authSetting, role: "postgres" }],
    [workerSetting, dbWide, authSetting, { ...authSetting, role: admit }],
  ])
    expect(verdict(after({ settings }))).toBe("release_bootstrap_readback");
  // Any role's validity changed, or a created role with VALID UNTIL.
  for (const name of ["authenticator", "bulls96_read_api", admit]) {
    const a = after();
    a.roles = a.roles.map((r) => (r.rolname === name ? { ...r, rolvaliduntil: "2030-01-01 00:00:00+00" } : r));
    expect(verdict(a)).toBe("release_bootstrap_readback");
  }
});

test("apply guard lists absent target-required chain roles only; psql meta lines are stripped", () => {
  const lines: string[] = [];
  const all = targetRequiredRoles(target);
  expect(bsCode(() => requireBootstrapRoles(bsState([admit, "bulls96_digest_narrative"]), all, (l) => lines.push(l)))).toBe(
    "release_bootstrap_required",
  );
  expect(lines).toEqual([`release_bootstrap_required ${admit},bulls96_digest_narrative`]);
  expect(() => requireBootstrapRoles(bsState(), all, quiet)).not.toThrow();
  // A role first consumed above the target is not required by the guard.
  const mid = targetRequiredRoles("upstream:20261105045040_selfhost_output_facts.sql");
  expect(mid).not.toContain(admit);
  expect(() => requireBootstrapRoles(bsState([admit]), mid, quiet)).not.toThrow();
  expect(bsCode(() => requireBootstrapRoles(bsState(["bulls96_digest_narrative"]), mid, quiet))).toBe("release_bootstrap_required");
  expect(stripPsqlMeta("\\set ON_ERROR_STOP on\nDO $x$ BEGIN END $x$;\n\\echo hi\n")).toBe("DO $x$ BEGIN END $x$;\n");
});

test("chain files never end or split the stage transaction", () => {
  // Every current chain file passes, as the stage sends it (psql meta lines stripped).
  expect(SESSION_BOOTSTRAP_FILES.length).toBe(5);
  for (const f of SESSION_BOOTSTRAP_FILES) {
    const text = stripPsqlMeta(readFileSync(new URL("../../../" + f, import.meta.url), "utf8"));
    expect(requireTransactionSafeChainSql(text)).toBe(text);
    // Its top level is only DO statements: every body and comment is stripped.
    expect(chainSqlTopLevel(text).split(";").map((t) => t.trim()).filter(Boolean).every((t) => t === "DO")).toBe(true);
  }
  expect(requireTransactionSafeChainSql("DO $x$ BEGIN\n  NULL;\nEND $x$;")).toContain("NULL");
  // (c) a DO body whose plpgsql ends with END, newline, then the closing tag passes.
  for (const ok of [
    "DO $$\nBEGIN\n  NULL;\nEND\n$$;",
    "DO $bootstrap$\nDECLARE v int;\nBEGIN\n  IF true THEN NULL; END IF;\nEND\n$bootstrap$;\n",
    "DO $a$ BEGIN NULL; END $a$;\nDO $b$ BEGIN NULL; END $b$;",
    // END/BEGIN only inside quotes, identifiers or comments at the top level.
    "SELECT 'END;';\nSELECT 1 AS \"end\";\n-- END;\n/* END; /* nested */ END; */ SELECT 2;",
    "SELECT E'it\\'s END;';",
    "SELECT CASE WHEN true THEN 1 END;",
  ])
    expect(requireTransactionSafeChainSql(ok)).toBe(ok);
  const unsafe = (sql: string) => bsCode(() => requireTransactionSafeChainSql(sql));
  for (const bad of ["COMMIT;", "rollback;", "BEGIN;", "begin transaction;", "SAVEPOINT a;", "START TRANSACTION;", "ABORT;", "PREPARE TRANSACTION 'x';", "RELEASE a;", "release savepoint a;"])
    expect(unsafe("DO $x$ BEGIN NULL; END $x$;\n" + bad)).toBe("release_bootstrap_chain_unsafe");
  // (a) a DO block followed by a top-level END; (COMMIT's synonym), then more statements.
  expect(unsafe("DO $x$ BEGIN NULL; END $x$;\nEND;\nCREATE ROLE rogue_role;")).toBe("release_bootstrap_chain_unsafe");
  // (b) every spelling of END as a statement.
  for (const end of ["end ;", "END WORK;", "END TRANSACTION;", "End\n;", "END", "  end work ;", "BEGIN WORK", "begin isolation level serializable;"])
    expect(unsafe("DO $$ BEGIN NULL; END $$;\n" + end)).toBe("release_bootstrap_chain_unsafe");
  // (d) strict on purpose: COMMIT/ROLLBACK/... are refused even as a comment word
  // inside a dollar body, as before; the chain files never contain them.
  expect(unsafe("DO $x$\nBEGIN\n  -- never COMMIT here\n  NULL;\nEND\n$x$;")).toBe("release_bootstrap_chain_unsafe");
  expect(unsafe("DO $x$ BEGIN RAISE NOTICE 'rollback'; END $x$;")).toBe("release_bootstrap_chain_unsafe");
  // Lexer evasions: a quote, identifier or E-string that would hide a top-level END.
  for (const evade of [
    "SELECT 1 AS foo$$;\nEND;\nSELECT $$;",
    "SELECT 'it''s $$';\nEND;",
    "SELECT E'\\'';\nEND;",
    "SELECT \"a\"\"b\";\nEND;",
    "/* /* */ */ END;",
    "DO $x$ BEGIN NULL; END $x$ ; END ; DO $y$ BEGIN NULL; END $y$;",
  ])
    expect(unsafe(evade)).toBe("release_bootstrap_chain_unsafe");
  // Unterminated comment, quote, identifier or body refuses (fail closed).
  for (const open of ["DO $x$ BEGIN NULL; END;", "SELECT 'x;", "SELECT \"x;", "/* x", "SELECT E'x\\';"])
    expect(unsafe(open)).toBe("release_bootstrap_chain_unsafe");
  // A positional parameter is not a dollar tag.
  expect(chainSqlTopLevel("SELECT $1, $2;")).toBe("SELECT $1, $2;");
});

test("bootstrap pre-connect: the anchor-bound chain equals the plan's and the approval's CHAIN", () => {
  const planned = bsSection({ files: bsFiles(), state: bsState([admit]), supabaseMode: true });
  const chain = (x: { files?: ReturnType<typeof bsFiles>; target?: string; targetSha?: string; steps?: string[] } = {}) =>
    bootstrapTargetChain({ target: x.target ?? target, targetSha: x.targetSha ?? targetSha, steps: x.steps ?? bsSteps, files: x.files ?? bsFiles(), contract: bsContract });
  expect(chain().chain_digest).toBe(planned.chain_digest);
  expect(requireBootstrapTargetChain(chain(), planned, planned.chain_digest)).toBe(planned.chain_digest);
  // Changed anchor bytes, another target or target_sha, or an approval/plan CHAIN that is not the live digest.
  for (const live of [
    chain({ files: bsFiles({}, { [readApiFile]: bsBytes(readApiFile) + "\n-- moved\n" }) }),
    chain({ target: "upstream:20261105046000_analysis_jobs.sql" }),
    chain({ targetSha: "7".repeat(40) }),
  ])
    expect(bsCode(() => requireBootstrapTargetChain(live, planned, planned.chain_digest))).toBe("release_bootstrap_plan_changed");
  expect(bsCode(() => requireBootstrapTargetChain(chain(), planned, "0".repeat(64)))).toBe("release_bootstrap_plan_changed");
  expect(bsCode(() => requireBootstrapTargetChain(chain(), { ...planned, chain_digest: "0".repeat(64) }, planned.chain_digest))).toBe(
    "release_bootstrap_plan_changed",
  );
  // Release-sha bytes never change the digest; a differing running file refuses as drift.
  const driftFiles = bsFiles({ [readApiFile]: "-- release edit\n" });
  const drift = chain({ files: driftFiles });
  expect(drift.chain_digest).toBe(planned.chain_digest);
  const driftPlan = bsSection({ files: driftFiles, state: bsState([admit]) });
  const lines: string[] = [];
  expect(bsCode(() => requireBootstrapTargetChain(drift, driftPlan, planned.chain_digest, (l) => lines.push(l)))).toBe(
    "release_bootstrap_release_drift",
  );
  expect(lines).toEqual([`release_bootstrap_release_drift ${readApiFile}`]);
  expect(bsCode(() => chain({ files: bsFiles({}, { [readApiFile]: undefined }) }))).toBe("release_bootstrap_bytes_missing");
  expect(bsCode(() => chain({ files: bsFiles().reverse() }))).toBe("release_bootstrap_chain");
  expect(bsCode(() => chain({ targetSha: "zz" }))).toBe("release_bootstrap_chain");
  // The target or a required first consumer missing from the anchor's steps.
  expect(bsCode(() => chain({ steps: bsSteps.filter((s) => s !== target) }))).toBe("release_bootstrap_manifest");
  expect(bsCode(() => chain({ steps: bsSteps.filter((s) => !s.includes("20261030020000_")) }))).toBe("release_bootstrap_manifest");
});

test("bootstrap mode binds the plan artifact's section into the typed approval", () => {
  const section = bsSection({ files: bsFiles(), state: bsState([admit]), supabaseMode: true });
  const input = { sha, generation, boundary: "global-default-revoke", target, targetSha, targetGeneration, planRun: "90", chainDigest: section.chain_digest, bootstrapRoles: [admit] };
  // validateReleasePlan's own refusals come first for a malformed plan.
  expect(bsCode(() => validateBootstrapPlan({ bootstrap: section }, input))).toBe("release_plan_artifact");
});

test("bootstrap entry point: every pre-connect check before the connection, the stage owns the transaction", () => {
  const src = readFileSync(new URL("../../../scripts/schema-release-database.ts", import.meta.url), "utf8");
  const entry = src.slice(src.indexOf("async function releaseBootstrap("), src.indexOf("export async function releaseDatabase("));
  const order = [
    "validateBootstrapPlan(",
    "const anchor = await targetAnchor(input, release.held);",
    "refuse(\"release_target_manifest_changed\");",
    "releaseBootstrapFiles(process.cwd(), input.sha, input.targetSha);",
    "const contract = releaseBootstrapContract(process.cwd(), input.targetSha);",
    "bootstrapTargetChain({ target: input.target, targetSha: input.targetSha, steps: anchorSteps(anchor), files, contract }),",
    "input.chainDigest,\n  );",
    "refuse(\"release_bootstrap_db_mode\");",
    "releaseClient(process.env.SUPABASE_MIGRATION_OWNER_URL);",
    "await client.connect();",
    "await requireReleaseOwner(client);",
    "await runBootstrapStage({",
    "approvalChain: input.chainDigest,",
    "await githubReleaseGate(process.env, get, true);",
    "const fresh = await targetAnchor(input, release.held);",
    "steps: anchorSteps(fresh),",
    "contract: releaseBootstrapContract(process.cwd(), input.targetSha),",
  ].map((s) => entry.indexOf(s));
  expect(order.every((i) => i > 0)).toBe(true);
  expect([...order].sort((a, b) => a - b)).toEqual(order);
  // The entry point itself issues no transaction control or chain SQL; the stage
  // (driven with a fake client in schema-release-bootstrap-stage.test.ts) does.
  expect(entry).not.toMatch(/"(BEGIN|COMMIT|ROLLBACK)"|pg_advisory|client\.query/);
  expect(src).toContain('if (input.mode === "bootstrap") return releaseBootstrap(get, input);');
  // Apply: the live absent-role guard runs inside beforeWrite under the migrator lock.
  expect(src).toMatch(/beforeWrite: async \(\) => \{[\s\S]*?const fresh = await targetAnchor\(input, approvedPlan!\.held\);[\s\S]*?await requireBootstrapRolesLive\(client, input\.target, fresh\);[\s\S]*?refreshReleaseBoundary/);
  expect(src).toContain("bootstrap,\n          },");
  expect(src).toContain("if (bootstrap.required) console.log(BOOTSTRAP_REQUIRED_NOTE);");
});

test("workflow offers the bootstrap mode in the same protected job with one secret step", () => {
  const w = readFileSync(new URL("../../../.github/workflows/apply-session-schema.yml", import.meta.url), "utf8");
  const parsed = Bun.YAML.parse(w) as any;
  expect(parsed.on.workflow_dispatch.inputs.mode.options).toEqual(["plan", "bootstrap", "apply"]);
  expect(parsed["run-name"]).toContain("${{ inputs.mode }}");
  expect(parsed.permissions).toEqual({ contents: "read", actions: "read", deployments: "read" });
  const steps = parsed.jobs.database.steps as any[];
  expect(parsed.jobs.database.environment.name).toBe("${{ needs.prerequisite-gate.outputs.environment }}");
  expect(steps.find((s) => s.uses === "actions/download-artifact@v4").if).toBe("inputs.mode != 'plan'");
  const secret = steps.filter((s) => s.env?.SUPABASE_MIGRATION_OWNER_URL);
  expect(secret).toHaveLength(1);
  expect(secret[0].run).toBe("bun scripts/schema-release-database.ts");
  const evidence = steps.find((s) => s.with?.name === "schema-release-bootstrap");
  expect(evidence.if).toBe("inputs.mode == 'bootstrap' && always()");
  expect(evidence.with.path).toBe("release-bootstrap/evidence.json");
  const summary = steps.find((s) => s.name === "Summary");
  expect(summary.run).toContain('"$RELEASE_MODE"');
  expect(summary.run).not.toContain("${{");
  expect(parsed.on.workflow_dispatch.inputs.approval.description).toContain(
    "BOOTSTRAP <project> <sha> TARGET <target> <target_sha> CHAIN <chain_digest> ROLES <sorted comma-joined will_create, or none> PLAN <plan_run>",
  );
  expect(parsed.on.workflow_dispatch.inputs.approval.description).toContain(
    "chain_digest from the plan binds target, target_sha, the target-required roles, the bootstrap contract digest and the anchor bytes of the run files",
  );
});

test("bootstrap mode passes the same GitHub prerequisite gate as apply, plan run included", async () => {
  const env = {
    ...base,
    RELEASE_MODE: "bootstrap",
    RELEASE_PLAN_RUN: "90",
    RELEASE_TARGET_GENERATION: targetGeneration,
    RELEASE_APPROVAL: bootstrapApproval({ project: P.project, sha, target, targetSha, chainDigest: "c".repeat(64), willCreate: [admit], planRun: "90" }),
  };
  expect((await githubReleaseGate(env, get(api()), true)).mode).toBe("bootstrap");
  for (const values of [
    { head_sha: "e".repeat(40) },
    { run_attempt: 2 },
    { conclusion: "failure" },
    { created_at: new Date(Date.now() - 25 * 3600000).toISOString() },
    { path: "wrong" },
  ]) {
    const f = api();
    Object.assign(f.plan, values);
    await expect(githubReleaseGate(env, get(f), true)).rejects.toThrow("release_plan_run");
  }
  const moved = api();
  await expect(
    githubReleaseGate({ ...env, GITHUB_RUN_ATTEMPT: "2" }, get(moved), true),
  ).rejects.toThrow("release_context");
});
