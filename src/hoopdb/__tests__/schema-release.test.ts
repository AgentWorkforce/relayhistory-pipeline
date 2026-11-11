import { test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import {
  releaseInputs,
  releaseConnection,
  SCHEMA_RELEASE as P,
} from "../schema-release-contract.ts";
import { githubReleaseGate } from "../../../scripts/schema-release-github.ts";
import { validateReleasePlan } from "../../../scripts/schema-release-database.ts";
import {
  manifestGeneration,
  migrationTargetSplit,
  RELEASE_TREE,
  releaseManifestDigest,
} from "../migrate.ts";
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
    /beforeWrite: async \(\) => \{\n\s+await githubReleaseGate\(process\.env, get, true\);\n(\s+\/\/[^\n]*\n)*\s+if \(!sameAnchor\(await targetAnchor\(input, approvedPlan!\.held\), approvedPlan!\)\)\n\s+refuse\("release_target_manifest_changed"\);/,
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
