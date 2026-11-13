/**
 * GitHub gate, contract validators and workflow shape for the exact-seven owner-buffered parity workflow
 * (scripts/owner-buffered-parity-github.ts, src/migration/owner-buffered-parity-ci-contract.ts,
 * .github/workflows/owner-buffered-parity.yml). GitHub is mocked; nothing here reaches the network.
 */
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { OWNER_PARITY_REQUIRED_PUSH_RUNS, ownerParityGithubGate } from "../../../scripts/owner-buffered-parity-github.ts";
import { SCHEMA_RELEASE as P } from "../../hoopdb/schema-release-contract.ts";
import { PRIVATE_INGEST_RELEASE } from "../../session-ingest/private-ingest-release.ts";
import {
  OWNER_PARITY_CENSUS_ARTIFACT,
  OWNER_PARITY_CENSUS_CONTRACT,
  OWNER_PARITY_EXACT_COUNT,
  OWNER_PARITY_MODES,
  OWNER_PARITY_ROSTER_FILE,
  OWNER_PARITY_WORKFLOW,
  OWNER_PARITY_WRITE_ENABLED,
  keepRosterTenants,
  ownerParityCensusDescriptor,
  ownerParityConfirmation,
  ownerParityDispatch,
  parseOwnerParityCensus,
  parseOwnerParityRoster,
  verifyOwnerParityWriteInputs,
  type OwnerParityCensusArtifact,
  type OwnerParityCensusBody,
} from "../owner-buffered-parity-ci-contract.ts";

const sha = "1".repeat(40), censusRun = "424242", G = "a".repeat(64);
const code = async (fn: () => unknown) => {
  try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};
const digests = Array.from({ length: 7 }, (_, i) => String(i + 1).repeat(64));
const roster = { contract: "owner-buffered-parity-roster/v1", tenants: digests };

function censusArtifact(over: Partial<OwnerParityCensusBody> = {}): OwnerParityCensusArtifact {
  const body: OwnerParityCensusBody = {
    contract: OWNER_PARITY_CENSUS_CONTRACT, sourceSha: sha, generation: G, censusSource: "frozen-supabase",
    capturedAt: new Date(Date.parse("2026-11-13T00:00:00.000Z")).toISOString(), censusRun,
    tenants: digests.map((d) => ({ tenantSha256: d, status: "equal", census: { tenantSha256: d, equal: true, mismatches: [] } })),
    ...over,
  };
  return { ...body, descriptorSha256: ownerParityCensusDescriptor(body) };
}
const phrase = ownerParityConfirmation(censusArtifact());

function githubFixture() {
  const actor = { id: P.dispatcher.id, login: P.dispatcher.login, type: "User" };
  const run = { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: OWNER_PARITY_WORKFLOW,
    run_attempt: 1, actor, triggering_actor: { ...actor }, created_at: new Date().toISOString() };
  return {
    ref: { object: { sha } }, run,
    environment: { id: 7, name: P.environment, protection_rules: [{ type: "branch_policy" }] as any[],
      deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } },
    branches: { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] },
    runs: { workflow_runs: [{ id: 10, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] },
    jobs: { total_count: 4, jobs: ["checks", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
      .map((name) => ({ name, head_sha: sha, status: "completed", conclusion: "success" })) },
    approvals: [] as any[],
    census: { ...run, actor: { ...actor }, triggering_actor: { ...actor }, id: Number(censusRun), status: "completed",
      conclusion: "success", created_at: new Date(Date.now() - 3600_000).toISOString() },
    artifacts: { total_count: 1, artifacts: [{ name: OWNER_PARITY_CENSUS_ARTIFACT, expired: false }] as any[] },
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
const env = (mode: string, extra: Record<string, string> = {}) =>
  ({ ...context, OWNER_PARITY_MODE: mode, OWNER_PARITY_SHA: sha, OWNER_PARITY_CENSUS_SOURCE: "frozen-supabase", ...extra });
const writeEnv = env("write", { OWNER_PARITY_CENSUS_RUN: censusRun, OWNER_PARITY_CONFIRMATION: phrase });
const gate = (e: Record<string, string>, f: Fixture, enabled = true, seen?: string[]) =>
  code(() => ownerParityGithubGate(e, githubGet(f, seen), true, { enabled }));

test("dormant gate: write refuses before any GitHub read while disabled; census and readback pass every gate", async () => {
  expect(OWNER_PARITY_WRITE_ENABLED).toBe(false);
  const seen: string[] = [];
  expect(await gate(writeEnv, githubFixture(), false, seen)).toBe("owner_parity_write_dormant");
  expect(seen).toEqual([]);
  expect(await code(() => ownerParityGithubGate(writeEnv, githubGet(githubFixture())))).toBe("owner_parity_write_dormant");
  expect(await gate(env("census"), githubFixture(), false)).toBeNull();
  expect(await gate(env("readback"), githubFixture(), false)).toBeNull();
  // Discovery passes exactly the census gates (exact main, pinned dispatcher, environment, green CI) and never
  // reads a census run.
  const discoverySeen: string[] = [];
  expect(await gate(env("discovery"), githubFixture(), false, discoverySeen)).toBeNull();
  expect(discoverySeen.length).toBeGreaterThan(0);
  expect(discoverySeen.some((path) => path.includes("/artifacts") || path.includes("/actions/runs/" + censusRun))).toBe(false);
  expect(await gate(env("discovery", { OWNER_PARITY_CENSUS_RUN: censusRun }), githubFixture())).toBe("owner_parity_census_run");
  expect(await gate(env("discovery", { OWNER_PARITY_CONFIRMATION: "x" }), githubFixture())).toBe("owner_parity_confirmation");
  expect(await gate(writeEnv, githubFixture())).toBeNull();
});

test("only census, write, readback and discovery are modes: bulk and anything else refuse before any GitHub read", async () => {
  expect([...OWNER_PARITY_MODES]).toEqual(["census", "write", "readback", "discovery"]);
  for (const mode of ["bulk", "BULK", "census ", "", "renew", "Discovery", "discovery "]) {
    const seen: string[] = [];
    expect(await gate(env(mode), githubFixture(), true, seen)).toBe("owner_parity_inputs");
    expect(seen).toEqual([]);
  }
  expect(await gate(env("census", { OWNER_PARITY_SHA: "1".repeat(39) }), githubFixture())).toBe("owner_parity_inputs");
  expect(await gate(env("census", { OWNER_PARITY_CENSUS_SOURCE: "supabase" }), githubFixture())).toBe("owner_parity_census_source");
  expect(await gate(env("census", { OWNER_PARITY_CENSUS_RUN: censusRun }), githubFixture())).toBe("owner_parity_census_run");
  expect(await gate(env("readback", { OWNER_PARITY_CONFIRMATION: phrase }), githubFixture())).toBe("owner_parity_confirmation");
});

test("stale main, CI, attempt, dispatcher, census run, artifact, environment and approval refuse the gate", async () => {
  const failures: Array<[Record<string, string>, (f: Fixture) => void, string]> = [
    [writeEnv, (f) => { f.ref.object.sha = "b".repeat(40); }, "owner_parity_gate_main_moved"],
    [{ ...writeEnv, GITHUB_SHA: "b".repeat(40) }, () => {}, "owner_parity_gate_context"],
    [{ ...writeEnv, GITHUB_RUN_ATTEMPT: "2" }, () => {}, "owner_parity_gate_context"],
    [{ ...writeEnv, GITHUB_REF: "refs/heads/dev" }, () => {}, "owner_parity_gate_context"],
    [{ ...writeEnv, GITHUB_EVENT_NAME: "push" }, () => {}, "owner_parity_gate_context"],
    [{ ...writeEnv, GITHUB_REPOSITORY_ID: "1" }, () => {}, "owner_parity_gate_context"],
    [{ ...writeEnv, GITHUB_REPOSITORY: "someone/fork" }, () => {}, "owner_parity_gate_context"],
    [writeEnv, (f) => { f.run.run_attempt = 2; }, "owner_parity_gate_run"],
    [writeEnv, (f) => { f.run.actor.id = 42; }, "owner_parity_gate_run"],
    [writeEnv, (f) => { f.run.triggering_actor.login = "someone"; }, "owner_parity_gate_run"],
    [writeEnv, (f) => { f.run.triggering_actor.type = "Bot"; }, "owner_parity_gate_run"],
    [writeEnv, (f) => { f.run.path = ".github/workflows/ingest-admission-renewal.yml"; }, "owner_parity_gate_run"],
    [writeEnv, (f) => { f.environment.deployment_branch_policy.protected_branches = true; }, "owner_parity_gate_environment"],
    [writeEnv, (f) => { f.branches.branch_policies.push({ name: "dev", type: "branch" }); f.branches.total_count = 2; }, "owner_parity_gate_environment_branch"],
    [writeEnv, (f) => { f.runs.workflow_runs[0]!.conclusion = "failure"; }, "owner_parity_gate_ci"],
    [writeEnv, (f) => { f.jobs.jobs = f.jobs.jobs.filter((j) => j.name !== "real PostgreSQL 16"); f.jobs.total_count = 3; }, "owner_parity_gate_ci_jobs"],
    [writeEnv, (f) => { f.jobs.jobs = f.jobs.jobs.filter((j) => j.name !== "checks"); f.jobs.total_count = 3; }, "owner_parity_gate_ci_jobs"],
    [writeEnv, (f) => { f.jobs.jobs[0]!.conclusion = "failure"; }, "owner_parity_gate_ci_jobs"],
    [writeEnv, (f) => { f.census.head_sha = "b".repeat(40); }, "owner_parity_gate_census_run"],
    [writeEnv, (f) => { f.census.run_attempt = 2; }, "owner_parity_gate_census_run"],
    [writeEnv, (f) => { f.census.conclusion = "failure"; }, "owner_parity_gate_census_run"],
    [writeEnv, (f) => { f.census.status = "in_progress"; }, "owner_parity_gate_census_run"],
    [writeEnv, (f) => { f.census.path = ".github/workflows/ingest-admission-renewal.yml"; }, "owner_parity_gate_census_run"],
    [writeEnv, (f) => { f.census.actor.id = 42; }, "owner_parity_gate_census_run"],
    [writeEnv, (f) => { f.census.created_at = new Date(Date.now() - 24 * 3600_000 - 1000).toISOString(); }, "owner_parity_gate_census_run"],
    [writeEnv, (f) => { f.census.created_at = new Date(Date.now() + 3600_000).toISOString(); }, "owner_parity_gate_census_run"],
    [writeEnv, (f) => { f.artifacts.artifacts[0]!.expired = true; }, "owner_parity_gate_census_artifact"],
    [writeEnv, (f) => { f.artifacts.artifacts.push({ name: OWNER_PARITY_CENSUS_ARTIFACT, expired: false }); }, "owner_parity_gate_census_artifact"],
    [writeEnv, (f) => { f.artifacts.artifacts = []; }, "owner_parity_gate_census_artifact"],
    [writeEnv, (f) => { f.environment.protection_rules.push({ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: 5 } }] }); },
      "owner_parity_gate_human_approval"],
    [env("census"), (f) => { f.run.actor.login = "someone"; }, "owner_parity_gate_run"],
    [env("readback"), (f) => { f.ref.object.sha = "b".repeat(40); }, "owner_parity_gate_main_moved"],
  ];
  for (const [input, mutate, expected] of failures) {
    const f = githubFixture();
    mutate(f);
    expect(await gate(input, f)).toBe(expected);
  }
});

test("an untrusted cross-session dispatcher refuses: pinned login and id on both actor and triggering_actor", async () => {
  for (const mutate of [
    (f: Fixture) => { f.run.actor = { id: 99, login: "relay-bot", type: "User" }; },
    (f: Fixture) => { f.run.triggering_actor = { id: 99, login: "relay-bot", type: "User" }; },
    (f: Fixture) => { f.run.actor.login = "khaliqgant-agent"; },
  ]) {
    const f = githubFixture();
    mutate(f);
    expect(await gate(env("census"), f)).toBe("owner_parity_gate_run");
  }
});

test("required push checks are exactly ci checks and pg-proof real PostgreSQL 15/16/17", () => {
  expect(OWNER_PARITY_REQUIRED_PUSH_RUNS.map(([w, n]) => [w, [...n]])).toEqual([
    ["ci.yml", ["checks"]], ["pg-proof.yml", ["real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]],
  ]);
});

test("confirmation shape: sha, source and census run are bound before any GitHub read", async () => {
  for (const bad of [phrase.replace(`${sha} GENERATION`, `${"2".repeat(40)} GENERATION`), phrase.replace(`CENSUS ${censusRun}`, "CENSUS 1"),
    phrase.replace("SOURCE frozen-supabase", "SOURCE neon"), phrase + " ", "write-owner-parity " + phrase.slice(19)]) {
    const seen: string[] = [];
    expect(await gate({ ...writeEnv, OWNER_PARITY_CONFIRMATION: bad }, githubFixture(), true, seen)).toBe("owner_parity_confirmation");
    expect(seen).toEqual([]);
  }
  expect(await gate(env("write", { OWNER_PARITY_CONFIRMATION: phrase }), githubFixture())).toBe("owner_parity_census_run");
});

test("roster: committed roster ships empty; exactly seven distinct digests; unlisted candidates are dropped", () => {
  const committed = JSON.parse(readFileSync(OWNER_PARITY_ROSTER_FILE, "utf8"));
  expect(committed.tenants).toEqual([]);
  expect(code(() => parseOwnerParityRoster(committed))).resolves.toBe("roster_empty");
  expect(OWNER_PARITY_EXACT_COUNT).toBe(7);
  expect(parseOwnerParityRoster(roster)).toEqual(digests);
  const refused = (v: unknown) => { try { parseOwnerParityRoster(v); return null; } catch (e) { return (e as { code: string }).code; } };
  expect(refused({ ...roster, tenants: digests.slice(0, 6) })).toBe("roster_incomplete");
  expect(refused({ ...roster, tenants: [...digests, "f".repeat(64)] })).toBe("roster_incomplete");
  expect(refused({ ...roster, tenants: [...digests.slice(0, 6), digests[0]] })).toBe("roster_invalid");
  expect(refused({ ...roster, tenants: [...digests.slice(0, 6), "X".repeat(64)] })).toBe("roster_invalid");
  expect(refused({ ...roster, orgIds: [] })).toBe("roster_invalid");
  expect(refused({ ...roster, contract: "v0" })).toBe("roster_invalid");
  const other = "e".repeat(64);
  const kept = keepRosterTenants([...digests].reverse().map((d) => ({ digest: d, tenant: d })).concat([{ digest: other, tenant: other }]), digests);
  expect(kept.map((k) => k.digest)).toEqual(digests);
  const keepRefused = (c: Array<{ digest: string; tenant: string }>) => { try { keepRosterTenants(c, digests); return null; } catch (e) { return (e as { code: string }).code; } };
  expect(keepRefused(digests.slice(1).map((d) => ({ digest: d, tenant: d })))).toBe("roster_incomplete");
  expect(keepRefused([...digests, digests[0]!].map((d) => ({ digest: d, tenant: d })))).toBe("roster_duplicate_candidate");
});

test("census artifact: descriptor binds every field; substitution, age, sha, source, run and roster drift refuse", () => {
  const now = Date.parse("2026-11-13T01:00:00.000Z");
  const ok = censusArtifact();
  const dispatch = ownerParityDispatch(writeEnv, true);
  expect(() => verifyOwnerParityWriteInputs(dispatch, parseOwnerParityCensus(ok), digests, now)).not.toThrow();
  const refused = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return (e as { code: string }).code; } };
  // Substitutions without recomputing the descriptor.
  for (const forge of [
    (c: any) => { c.tenants[0].tenantSha256 = c.tenants[0].census.tenantSha256 = "e".repeat(64); },
    (c: any) => { c.generation = "b".repeat(64); },
    (c: any) => { c.capturedAt = "2026-11-13T00:30:00.000Z"; },
    (c: any) => { c.tenants[1].census.mismatches = ["head"]; },
  ]) {
    const c = JSON.parse(JSON.stringify(ok));
    forge(c);
    expect(refused(() => parseOwnerParityCensus(c))).toBe("owner_parity_census_descriptor");
  }
  expect(refused(() => parseOwnerParityCensus({ ...ok, orgId: "x" }))).toBe("owner_parity_census_invalid");
  expect(refused(() => parseOwnerParityCensus({ ...ok, tenants: ok.tenants.slice(0, 6) }))).toBe("owner_parity_census_invalid");
  const verify = (c: OwnerParityCensusArtifact, at = now, d = dispatch, r = digests) => refused(() => verifyOwnerParityWriteInputs(d, c, r, at));
  expect(verify(ok, now + 24 * 3600_000)).toBe("owner_parity_census_stale");
  expect(verify(ok, Date.parse(ok.capturedAt) - 1)).toBe("owner_parity_census_stale");
  expect(verify(ok, Date.parse(ok.capturedAt) + 24 * 3600_000 - 1)).toBeNull();
  expect(verify(censusArtifact({ sourceSha: "2".repeat(40) }))).toBe("owner_parity_census_sha_mismatch");
  expect(verify(censusArtifact({ censusRun: "1" }))).toBe("owner_parity_census_run_mismatch");
  expect(verify(censusArtifact({ censusSource: "neon" }))).toBe("owner_parity_census_source_mismatch");
  expect(verify(ok, now, dispatch, [...digests].reverse())).toBe("owner_parity_census_roster_mismatch");
  const unequal = censusArtifact({ tenants: ok.tenants.map((t, i) => i ? t : { ...t, status: "mismatch", census: { ...t.census, equal: false } }) });
  expect(verify(unequal, now, ownerParityDispatch({ ...writeEnv, OWNER_PARITY_CONFIRMATION: ownerParityConfirmation(unequal) }, true)))
    .toBe("owner_parity_census_not_equal");
  const moved = censusArtifact({ generation: "b".repeat(64) });
  expect(verify(moved)).toBe("owner_parity_confirmation");
});

test("workflow: two pinned secrets in one step, gate without environment, proof without secrets, census 1d and receipt 30d", () => {
  const workflow = readFileSync(OWNER_PARITY_WORKFLOW, "utf8");
  expect(workflow.match(/secrets\.[A-Z_]+/gu)).toEqual([
    "secrets.SUPABASE_MIGRATION_OWNER_URL", "secrets.OWNER_PARITY_NEON_READONLY_URL",
  ]);
  expect(workflow.match(/vars\.[A-Za-z_]+/gu)).toBeNull();
  expect(workflow).toContain("options: [census, write, readback, discovery]");
  expect(workflow).toContain("options: [frozen-supabase, neon]");
  expect(workflow).toContain("default: census\n");
  expect(workflow).toContain("default: frozen-supabase\n");
  expect(workflow).toContain("group: ${{ github.event_name == 'workflow_dispatch' && 'guarded-session-schema-production'");
  expect(workflow).toContain("format('owner-buffered-parity-proof-{0}', github.sha)");
  expect(workflow).toContain("permissions:\n  contents: read\n  actions: read\n  deployments: read\n");
  const proof = workflow.slice(workflow.indexOf("  proof:"), workflow.indexOf("  prerequisite-gate:"));
  expect(proof).toContain("if: github.event_name != 'workflow_dispatch'");
  expect(proof).toContain("bun scripts/owner-buffered-parity-ci.ts --proof");
  expect(proof).not.toContain("secrets.");
  expect(proof).not.toContain("environment:");
  const gateJob = workflow.slice(workflow.indexOf("  prerequisite-gate:"), workflow.indexOf("  database:"));
  expect(gateJob).not.toContain("environment:\n");
  expect(gateJob).not.toContain("secrets.");
  expect(gateJob).toContain("github.ref == 'refs/heads/main'");
  expect(gateJob).toContain(`github.repository_id == '${PRIVATE_INGEST_RELEASE.repositoryId}'`);
  const db = workflow.slice(workflow.indexOf("  database:"));
  expect(db).toContain("environment:\n      name: ${{ needs.prerequisite-gate.outputs.environment }}\n");
  expect(workflow).toContain("echo \"environment=session-schema-production\"");
  expect(workflow.indexOf("owner-buffered-parity-github.ts --approved")).toBeLessThan(workflow.indexOf("secrets.SUPABASE_MIGRATION_OWNER_URL"));
  expect(workflow.indexOf("owner-buffered-parity-github.ts --approved")).toBeLessThan(workflow.indexOf("secrets.OWNER_PARITY_NEON_READONLY_URL"));
  expect(workflow.match(/persist-credentials: false/gu)?.length).toBe(3);
  expect(workflow.match(/uses: actions\/checkout@v4/gu)?.length).toBe(3);
  const census = workflow.slice(workflow.indexOf("Publish the census artifact"), workflow.indexOf("Publish the sanitized receipt"));
  expect(census).toContain(`name: ${OWNER_PARITY_CENSUS_ARTIFACT}\n`);
  expect(census).toContain("retention-days: 1\n");
  const receipt = workflow.slice(workflow.indexOf("Publish the sanitized receipt"));
  expect(receipt).toContain("name: owner-buffered-parity\n");
  expect(receipt).toContain("retention-days: 30\n");
  // The step holding the secret runs the wrapper only; the URL never reaches argv.
  const step = workflow.slice(workflow.indexOf("only step with the database secret"), workflow.indexOf("Publish the census artifact"));
  expect(step).toContain("run: bun scripts/owner-buffered-parity-ci.ts\n");
  expect(step).not.toMatch(/\$SUPABASE|\$\{SUPABASE|echo/u);
  for (const forbidden of ["supabase login", "SET ROLE", "neonUrl", "bulk", P.project]) expect(workflow).not.toContain(forbidden);
});
