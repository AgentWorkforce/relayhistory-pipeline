/**
 * GitHub gate, credential hand-off and workflow shape for the 058000 CONCURRENTLY index prebuild
 * (scripts/index-prebuild-058000-github.ts, scripts/index-prebuild-058000.ts, .github/workflows/index-prebuild-058000.yml),
 * in the style of the schema release and admission-renewal gate tests. GitHub is mocked; nothing reaches the network.
 */
import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { indexPrebuildGithubGate } from "../../../scripts/index-prebuild-058000-github.ts";
import {
  INDEX_PREBUILD_CHILD_ENV,
  indexPrebuildChildSpawn,
  refuseIndexPrebuildCredentialEnv,
} from "../../../scripts/index-prebuild-058000.ts";
import { SCHEMA_RELEASE as P } from "../schema-release-contract.ts";
import { PRIVATE_INGEST_RELEASE } from "../../session-ingest/private-ingest-release.ts";
import {
  INDEX_PREBUILD_CENSUS_ARTIFACT,
  INDEX_PREBUILD_ENABLED,
  INDEX_PREBUILD_MODES,
  INDEX_PREBUILD_RUN_BUDGET_MS,
  INDEX_PREBUILD_WORKFLOW,
} from "../index-prebuild-058000.ts";

const sha = "1".repeat(40), censusRun = "424242", G = "a".repeat(64);
const phrase = `PREBUILD-058000 ${sha} GENERATION ${G} TAIL upstream:20261105057030_selfhost_erase_directory_membership.sql LEDGER ${"b".repeat(64)} ` +
  `DESCRIPTOR ${"c".repeat(64)} CENSUS ${censusRun} INDEXES ingest_lazy_native_slots_current_seat_key_idx`;
const code = async (fn: () => unknown) => {
  try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};

function githubFixture() {
  const actor = { id: P.dispatcher.id, login: P.dispatcher.login, type: "User" };
  const run = { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: INDEX_PREBUILD_WORKFLOW,
    run_attempt: 1, actor, triggering_actor: { ...actor }, created_at: new Date().toISOString() };
  return {
    ref: { object: { sha } }, run,
    environment: { id: 7, name: P.environment, protection_rules: [{ type: "branch_policy" }] as any[],
      deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } },
    branches: { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] },
    runs: { workflow_runs: [{ id: 10, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] },
    jobs: { total_count: 5, jobs: ["checks", "session-read-worker / worker", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
      .map((name) => ({ name, head_sha: sha, status: "completed", conclusion: "success" })) },
    approvals: [] as any[],
    census: { ...run, actor: { ...actor }, triggering_actor: { ...actor }, id: Number(censusRun), status: "completed",
      conclusion: "success", created_at: new Date(Date.now() - 3600_000).toISOString() },
    artifacts: { total_count: 1, artifacts: [{ name: INDEX_PREBUILD_CENSUS_ARTIFACT, expired: false }] as any[] },
  };
}
type Fixture = ReturnType<typeof githubFixture>;
const githubGet = (f: Fixture, seen: string[] = []) => async (path: string): Promise<any> => {
  seen.push(path);
  if (path.endsWith("/git/ref/heads/main")) return f.ref;
  if (path.endsWith("/actions/runs/100")) return f.run;
  if (path.endsWith("/actions/runs/100/approvals")) return f.approvals;
  if (path.endsWith(`/actions/runs/${censusRun}`)) return f.census;
  if (path.endsWith(`/actions/runs/${censusRun}/artifacts?per_page=100`)) return f.artifacts;
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
const env = (mode: string, extra: Record<string, string> = {}) => ({ ...context, PREBUILD_MODE: mode, PREBUILD_SHA: sha, ...extra });
const prebuildEnv = env("prebuild", { PREBUILD_CENSUS_RUN: censusRun, PREBUILD_CONFIRMATION: phrase });
const gate = (e: Record<string, string>, f: Fixture, enabled = true, seen?: string[]) =>
  code(() => indexPrebuildGithubGate(e, githubGet(f, seen), true, { enabled }));

test("dormant: prebuild refuses before any GitHub read while disabled; census and readback pass every gate", async () => {
  expect(INDEX_PREBUILD_ENABLED).toBe(false);
  const seen: string[] = [];
  expect(await gate(prebuildEnv, githubFixture(), false, seen)).toBe("index_prebuild_dormant");
  expect(seen).toEqual([]);
  expect(await code(() => indexPrebuildGithubGate(prebuildEnv, githubGet(githubFixture())))).toBe("index_prebuild_dormant");
  expect(await gate(env("census"), githubFixture(), false)).toBeNull();
  expect(await gate(env("readback"), githubFixture(), false)).toBeNull();
  expect(await gate(prebuildEnv, githubFixture())).toBeNull();
});

test("only census, prebuild and readback: anything else refuses before any GitHub read", async () => {
  expect([...INDEX_PREBUILD_MODES]).toEqual(["census", "prebuild", "readback"]);
  for (const mode of ["apply", "drop", "PREBUILD", "census ", ""]) {
    const seen: string[] = [];
    expect(await gate(env(mode), githubFixture(), true, seen)).toBe("index_prebuild_inputs");
    expect(seen).toEqual([]);
  }
  expect(await gate(env("census", { PREBUILD_SHA: "1".repeat(39) }), githubFixture())).toBe("index_prebuild_inputs");
  expect(await gate(env("census", { PREBUILD_CENSUS_RUN: censusRun }), githubFixture())).toBe("index_prebuild_census_run");
  expect(await gate(env("readback", { PREBUILD_CONFIRMATION: phrase }), githubFixture())).toBe("index_prebuild_confirmation");
  expect(await gate(env("prebuild", { PREBUILD_CONFIRMATION: phrase }), githubFixture())).toBe("index_prebuild_census_run");
});

test("stale main, CI, attempt, dispatcher, census run, artifact, environment and approval refuse the gate", async () => {
  const failures: Array<[Record<string, string>, (f: Fixture) => void, string]> = [
    [prebuildEnv, (f) => { f.ref.object.sha = "b".repeat(40); }, "index_prebuild_gate_main_moved"],
    [{ ...prebuildEnv, GITHUB_SHA: "b".repeat(40) }, () => {}, "index_prebuild_gate_context"],
    [{ ...prebuildEnv, GITHUB_RUN_ATTEMPT: "2" }, () => {}, "index_prebuild_gate_context"],
    [{ ...prebuildEnv, GITHUB_REF: "refs/heads/dev" }, () => {}, "index_prebuild_gate_context"],
    [{ ...prebuildEnv, GITHUB_EVENT_NAME: "push" }, () => {}, "index_prebuild_gate_context"],
    [{ ...prebuildEnv, GITHUB_REPOSITORY_ID: "1" }, () => {}, "index_prebuild_gate_context"],
    [{ ...prebuildEnv, GITHUB_REPOSITORY: "someone/fork" }, () => {}, "index_prebuild_gate_context"],
    [prebuildEnv, (f) => { f.run.run_attempt = 2; }, "index_prebuild_gate_run"],
    [prebuildEnv, (f) => { f.run.actor.id = 42; }, "index_prebuild_gate_run"],
    [prebuildEnv, (f) => { f.run.triggering_actor.login = "someone"; }, "index_prebuild_gate_run"],
    [prebuildEnv, (f) => { f.run.triggering_actor.type = "Bot"; }, "index_prebuild_gate_run"],
    [prebuildEnv, (f) => { f.run.path = ".github/workflows/apply-session-schema.yml"; }, "index_prebuild_gate_run"],
    [prebuildEnv, (f) => { f.environment.deployment_branch_policy.protected_branches = true; }, "index_prebuild_gate_environment"],
    [prebuildEnv, (f) => { f.environment.protection_rules = undefined as never; }, "index_prebuild_gate_environment"],
    [prebuildEnv, (f) => { f.branches.branch_policies.push({ name: "dev", type: "branch" }); f.branches.total_count = 2; }, "index_prebuild_gate_environment_branch"],
    [prebuildEnv, (f) => { f.runs.workflow_runs[0]!.conclusion = "failure"; }, "index_prebuild_gate_ci"],
    [prebuildEnv, (f) => { f.runs.workflow_runs[0]!.head_branch = "dev"; }, "index_prebuild_gate_ci"],
    [prebuildEnv, (f) => { f.jobs.jobs = f.jobs.jobs.filter((j) => j.name !== "real PostgreSQL 16"); f.jobs.total_count = 4; }, "index_prebuild_gate_ci_jobs"],
    [prebuildEnv, (f) => { f.jobs.jobs = f.jobs.jobs.filter((j) => j.name !== "checks"); f.jobs.total_count = 4; }, "index_prebuild_gate_ci_jobs"],
    [prebuildEnv, (f) => { f.jobs.jobs = f.jobs.jobs.filter((j) => j.name !== "session-read-worker / worker"); f.jobs.total_count = 4; }, "index_prebuild_gate_ci_jobs"],
    [prebuildEnv, (f) => { f.jobs.jobs[0]!.conclusion = "failure"; }, "index_prebuild_gate_ci_jobs"],
    [prebuildEnv, (f) => { f.census.head_sha = "b".repeat(40); }, "index_prebuild_gate_census_run"],
    [prebuildEnv, (f) => { f.census.run_attempt = 2; }, "index_prebuild_gate_census_run"],
    [prebuildEnv, (f) => { f.census.conclusion = "failure"; }, "index_prebuild_gate_census_run"],
    [prebuildEnv, (f) => { f.census.status = "in_progress"; }, "index_prebuild_gate_census_run"],
    [prebuildEnv, (f) => { f.census.path = ".github/workflows/apply-session-schema.yml"; }, "index_prebuild_gate_census_run"],
    [prebuildEnv, (f) => { f.census.actor.id = 42; }, "index_prebuild_gate_census_run"],
    [prebuildEnv, (f) => { f.census.triggering_actor.login = "relay-bot"; }, "index_prebuild_gate_census_run"],
    [prebuildEnv, (f) => { f.census.created_at = new Date(Date.now() - 24 * 3600_000 - 1000).toISOString(); }, "index_prebuild_gate_census_run"],
    [prebuildEnv, (f) => { f.census.created_at = new Date(Date.now() + 3600_000).toISOString(); }, "index_prebuild_gate_census_run"],
    [prebuildEnv, (f) => { f.artifacts.artifacts[0]!.expired = true; }, "index_prebuild_gate_census_artifact"],
    [prebuildEnv, (f) => { f.artifacts.artifacts.push({ name: INDEX_PREBUILD_CENSUS_ARTIFACT, expired: false }); }, "index_prebuild_gate_census_artifact"],
    [prebuildEnv, (f) => { f.artifacts.artifacts = []; }, "index_prebuild_gate_census_artifact"],
    [prebuildEnv, (f) => { f.environment.protection_rules.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] }); },
      "index_prebuild_gate_human_approval"],
    [prebuildEnv, (f) => {
      f.environment.protection_rules.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] });
      f.approvals.push({ state: "approved", user: { type: "User", id: 5 }, environments: [{ id: 7, name: P.environment }] },
        { state: "rejected", user: { type: "User", id: 6 }, environments: [{ id: 7, name: P.environment }] });
    }, "index_prebuild_gate_human_rejected"],
    [env("census"), (f) => { f.run.actor.login = "someone"; }, "index_prebuild_gate_run"],
    [env("readback"), (f) => { f.ref.object.sha = "b".repeat(40); }, "index_prebuild_gate_main_moved"],
  ];
  for (const [input, mutate, expected] of failures) {
    const f = githubFixture();
    mutate(f);
    expect(`${expected} ${await gate(input, f)}`).toBe(`${expected} ${expected}`);
  }
  // A pinned reviewer's recorded approval for this environment passes.
  const ok = githubFixture();
  ok.environment.protection_rules.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] });
  ok.approvals.push({ state: "approved", user: { type: "User", id: 5 }, environments: [{ id: 7, name: P.environment }] });
  expect(await gate(prebuildEnv, ok)).toBeNull();
});

test("an untrusted dispatcher refuses even with environment reviewers: pinned login and id on actor and triggering_actor", async () => {
  for (const mutate of [
    (f: Fixture) => { f.run.actor = { id: 99, login: "relay-bot", type: "User" }; },
    (f: Fixture) => { f.run.triggering_actor = { id: 99, login: "relay-bot", type: "User" }; },
    (f: Fixture) => { f.run.actor.login = P.dispatcher.login + "-agent"; },
  ]) {
    const f = githubFixture();
    f.environment.protection_rules.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] });
    f.approvals.push({ state: "approved", user: { type: "User", id: 5 }, environments: [{ id: 7, name: P.environment }] });
    mutate(f);
    expect(await gate(env("census"), f)).toBe("index_prebuild_gate_run");
  }
});

test("confirmation shape: sha and census run are bound before any GitHub read", async () => {
  for (const bad of [phrase.replace(`${sha} GENERATION`, `${"2".repeat(40)} GENERATION`), phrase.replace(`CENSUS ${censusRun}`, "CENSUS 1"),
    "prebuild-058000 " + phrase.slice(16), phrase.replace(" INDEXES ", " INDEX ")]) {
    const seen: string[] = [];
    expect(await gate({ ...prebuildEnv, PREBUILD_CONFIRMATION: bad }, githubFixture(), true, seen)).toBe("index_prebuild_confirmation");
    expect(seen).toEqual([]);
  }
});

test("credential hand-off: the child gets the URL on stdin only, never argv or env; the child refuses a credential env", () => {
  const url = "postgresql://fixture-user:fixture-secret@db.fixture.invalid:5432/postgres";
  const parentEnv = { PATH: "/usr/bin", HOME: "/home/runner", RUNNER_TEMP: "/tmp/r", GITHUB_RUN_ID: "202", GITHUB_SHA: sha,
    GITHUB_TOKEN: "fixture-token", SUPABASE_MIGRATION_OWNER_URL: url, DATABASE_URL: url, ACTIONS_RUNTIME_TOKEN: "fixture-token",
    PREBUILD_MODE: "census", PREBUILD_SHA: sha };
  const spec = indexPrebuildChildSpawn(parentEnv, url, "/x/scripts/index-prebuild-058000.ts");
  expect(spec.args).toEqual(["/x/scripts/index-prebuild-058000.ts", "--operator-stdin"]);
  expect(Object.keys(spec.env).sort()).toEqual(["GITHUB_RUN_ID", "GITHUB_SHA", "HOME", "PATH", "PREBUILD_MODE", "PREBUILD_SHA", "RUNNER_TEMP"]);
  expect(JSON.stringify([spec.command, spec.args, spec.env])).not.toContain("fixture-secret");
  expect(JSON.stringify([spec.command, spec.args, spec.env])).not.toContain("fixture-token");
  expect(JSON.parse(spec.input)).toEqual({ supabaseUrl: url });
  for (const k of Object.keys(spec.env)) expect((INDEX_PREBUILD_CHILD_ENV as readonly string[]).includes(k)).toBe(true);
  expect(() => refuseIndexPrebuildCredentialEnv(spec.env)).not.toThrow();
  for (const extra of [{ SUPABASE_MIGRATION_OWNER_URL: url }, { GITHUB_TOKEN: "t" }, { PGPASSWORD: "p" }, { DATABASE_URL: url },
    { HOOPDB_ALLOW_BLOCKING_INDEX_BUILD: "true" }])
    expect(() => refuseIndexPrebuildCredentialEnv({ ...spec.env, ...extra })).toThrow();
});

test("parent process: a dormant prebuild refuses before the secret is used; nothing secret is printed or written", () => {
  const temp = mkdtempSync(join(tmpdir(), "index-prebuild-"));
  try {
    const url = "postgresql://fixture-user:fixture-secret@db.fixture.invalid:5432/postgres";
    const run = (extra: Record<string, string>) => spawnSync(process.execPath, ["scripts/index-prebuild-058000.ts"], {
      cwd: join(import.meta.dir, "../../.."), encoding: "utf8",
      env: { PATH: process.env.PATH ?? "", RUNNER_TEMP: temp, GITHUB_RUN_ID: "202", SUPABASE_MIGRATION_OWNER_URL: url, PREBUILD_SHA: sha, ...extra },
    });
    const dormant = run({ PREBUILD_MODE: "prebuild", PREBUILD_CENSUS_RUN: censusRun, PREBUILD_CONFIRMATION: phrase });
    expect(dormant.status).toBe(1);
    expect(dormant.stdout).toContain("\"code\":\"index_prebuild_dormant\"");
    const bad = run({ PREBUILD_MODE: "drop" });
    expect(bad.status).toBe(1);
    expect(bad.stdout).toContain("\"code\":\"index_prebuild_inputs\"");
    const receipt = readFileSync(join(temp, "index-prebuild-out", "index-prebuild-058000.json"), "utf8");
    for (const text of [dormant.stdout, dormant.stderr, bad.stdout, bad.stderr, receipt]) {
      expect(text).not.toContain("fixture-secret");
      expect(text).not.toContain("db.fixture.invalid");
    }
    expect(JSON.parse(receipt)).toMatchObject({ status: "refused", code: "index_prebuild_inputs", credentialsPersisted: false });
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
});

test("workflow: dormant source, one secret in one step, gate without environment, proof without secrets, no blocking flag, never applies 058000", () => {
  const workflow = readFileSync(INDEX_PREBUILD_WORKFLOW, "utf8");
  expect(workflow.match(/secrets\.[A-Z_]+/gu)).toEqual(["secrets.SUPABASE_MIGRATION_OWNER_URL"]);
  expect(workflow.match(/vars\.[A-Za-z_]+/gu)).toBeNull();
  expect(workflow).toContain("options: [census, prebuild, readback]");
  expect(workflow).toContain("default: census\n");
  expect(workflow).toContain("group: ${{ github.event_name == 'workflow_dispatch' && 'guarded-session-schema-production'");
  expect(workflow).toContain("cancel-in-progress: false");
  expect(workflow).toContain("permissions:\n  contents: read\n  actions: read\n  deployments: read\n");
  const proof = workflow.slice(workflow.indexOf("  proof:"), workflow.indexOf("  prerequisite-gate:"));
  expect(proof).toContain("if: github.event_name != 'workflow_dispatch'");
  expect(proof).not.toContain("secrets.");
  expect(proof).not.toContain("environment:");
  const gateJob = workflow.slice(workflow.indexOf("  prerequisite-gate:"), workflow.indexOf("  database:"));
  expect(gateJob).not.toContain("environment:\n");
  expect(gateJob).not.toContain("secrets.");
  expect(gateJob).toContain("github.ref == 'refs/heads/main'");
  expect(gateJob).toContain(`github.repository_id == '${PRIVATE_INGEST_RELEASE.repositoryId}'`);
  expect(gateJob).toContain("bun scripts/index-prebuild-058000-github.ts\n");
  const db = workflow.slice(workflow.indexOf("  database:"));
  expect(db).toContain("environment:\n      name: ${{ needs.prerequisite-gate.outputs.environment }}\n");
  expect(workflow).toContain(`echo "environment=${P.environment}"`);
  expect(workflow.indexOf("index-prebuild-058000-github.ts --approved")).toBeLessThan(workflow.indexOf("secrets.SUPABASE_MIGRATION_OWNER_URL"));
  expect(workflow.match(/persist-credentials: false/gu)?.length).toBe(3);
  expect(workflow.match(/uses: actions\/checkout@v4/gu)?.length).toBe(3);
  expect(workflow.match(/bun-version: 1\.4\.2/gu)?.length).toBe(3);
  const census = workflow.slice(workflow.indexOf("Publish the census artifact"), workflow.indexOf("Publish the sanitized receipt"));
  expect(census).toContain(`name: ${INDEX_PREBUILD_CENSUS_ARTIFACT}\n`);
  expect(census).toContain("retention-days: 1\n");
  const download = workflow.slice(workflow.indexOf("Download the census artifact"), workflow.indexOf("only step with the database secret"));
  expect(download).toContain("if: inputs.mode == 'prebuild'");
  expect(download).toContain("run-id: ${{ inputs.census_run }}");
  const receipt = workflow.slice(workflow.indexOf("Publish the sanitized receipt"));
  expect(receipt).toContain("if: always()");
  expect(receipt).toContain("retention-days: 30\n");
  // The job outlives the operator's run budget, so a build is never started that the job would kill.
  const minutes = Number(/timeout-minutes: ([0-9]+)\n    environment:/u.exec(workflow)?.[1]);
  expect(minutes * 60_000).toBeGreaterThan(INDEX_PREBUILD_RUN_BUDGET_MS);
  // The step holding the secret runs the wrapper only; the URL never reaches argv or a shell expansion.
  const step = workflow.slice(workflow.indexOf("only step with the database secret"), workflow.indexOf("Publish the census artifact"));
  expect(step).toContain("run: bun scripts/index-prebuild-058000.ts\n");
  expect(step).not.toMatch(/\$SUPABASE|\$\{SUPABASE|echo/u);
  // Never the blocking path, never the migrator, never the project ref.
  for (const forbidden of ["HOOPDB_ALLOW_BLOCKING_INDEX_BUILD", "allow_blocking_index_build", "src/hoopdb/migrate.ts",
    "schema-release-database.ts", "psql", "DROP INDEX", "supabase db", P.project])
    expect(workflow).not.toContain(forbidden);
  expect(workflow.match(/CREATE INDEX(?! CONCURRENTLY)/gu)).toBeNull();
});

test("pg-proof: the 058000 real-PostgreSQL step is skipped with a warning at an older PR head that lacks its test file", () => {
  const workflow = readFileSync(".github/workflows/pg-proof.yml", "utf8");
  const file = "src/hoopdb/__tests__/index-prebuild-058000.postgres.test.ts";
  const at = workflow.indexOf("- name: Prove the 058000 index prebuild");
  expect(at).toBeGreaterThan(-1);
  const step = workflow.slice(at, workflow.indexOf("\n      - name:", at + 1));
  const run = step.slice(step.indexOf("run: |\n") + "run: |\n".length).replace(/^ {10}/gmu, "");
  const guard = run.indexOf(`if [ ! -f ${file} ]; then`);
  expect(guard).toBeGreaterThan(-1);
  expect(guard).toBeLessThan(run.indexOf(`bun test --timeout 300000 ${file}`));
  // Execute the step's own script with bun test stubbed: absent file -> warning, exit 0, no test; present -> runs it.
  const script = run.replace(`bun test --timeout 300000 ${file}`, "echo RAN-TEST");
  expect(script).toContain("echo RAN-TEST");
  for (const present of [false, true]) {
    const dir = mkdtempSync(join(tmpdir(), "pg-proof-guard-"));
    try {
      if (present) {
        mkdirSync(join(dir, "src/hoopdb/__tests__"), { recursive: true });
        writeFileSync(join(dir, file), "");
      }
      const r = spawnSync("bash", ["-e", "-c", script], { cwd: dir, encoding: "utf8" });
      expect(r.status).toBe(0);
      expect(r.stdout.includes("RAN-TEST")).toBe(present);
      expect(r.stdout.includes("::warning title=pg-proof::index-prebuild-058000.postgres.test.ts absent")).toBe(!present);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }
});
