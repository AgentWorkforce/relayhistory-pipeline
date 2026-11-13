// Causal tests of the frozen-copy supervisor attestation (synthetic GitHub API, no network, no database).
import { test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  FROZEN_COPY_CREDENTIAL_PRODUCER, FROZEN_COPY_CREDENTIAL_PRODUCER_SHA256, FROZEN_COPY_RECEIPT_SCHEMA, FROZEN_CUSTODY,
  isSupervisorProof, supervisorContextRefusal, supervisorProofRefusal, verifyCustodyGithub, verifySupervisor,
  type CustodyAction, type SupervisorFacts,
} from "../frozen-source-copy-supervisor.ts";
import { FrozenCopyRefusal } from "../frozen-source-copy-seal.ts";
import { jobReceipt } from "../frozen-source-copy-progress.ts";
import { SCHEMA_RELEASE } from "../../hoopdb/schema-release-contract.ts";
import { PRIVATE_INGEST_RELEASE } from "../../session-ingest/private-ingest-release.ts";
import { testEnvelope } from "./fixtures/frozen-copy-envelope.ts";
import {
  PLAN_RUN, RUN_ID, SOURCE_SHA, fakeGithub, fixtureApproval, fixtureFacts, mintSupervisorProof, supervisorEnv, type Forge,
} from "./fixtures/frozen-copy-supervisor.ts";
import { frozenSourceCustodyGate } from "../../../scripts/frozen-source-custody-github.ts";

const root = (p: string) => fileURLToPath(new URL("../../../" + p, import.meta.url));
const yaml = readFileSync(root(".github/workflows/frozen-source-custody.yml"), "utf8");
const doc = readFileSync(root("docs/p1-frozen-source-custody.md"), "utf8");

async function refusal(p: Promise<unknown>): Promise<{ code: string; check: string } | null> {
  try {
    await p;
    return null;
  } catch (e) {
    expect(e).toBeInstanceOf(FrozenCopyRefusal);
    return { code: (e as FrozenCopyRefusal).code, check: (e as FrozenCopyRefusal).detail.check! };
  }
}
const attest = (action: CustodyAction, o: { env?: Record<string, string | undefined>; forge?: Forge; facts?: Partial<SupervisorFacts>; approval?: string } = {}) => {
  const f = { ...fixtureFacts(action), ...o.facts };
  const approval = o.approval ?? fixtureApproval(fixtureFacts(action), action === "seal" ? "e".repeat(64) : null);
  return verifySupervisor(supervisorEnv(action, approval, o.env), fakeGithub(o.forge, action), f);
};

test("happy path: a minted proof with a sanitized block, reading every GitHub route", async () => {
  const get = fakeGithub({}, "seal");
  const f = fixtureFacts("seal");
  const proof = await verifySupervisor(supervisorEnv("seal", fixtureApproval(f, "e".repeat(64))), get, f);
  expect(isSupervisorProof(proof)).toBe(true);
  expect(proof).toMatchObject({ action: "seal", manifest: "e".repeat(64), epoch: f.epoch, privacy: f.privacy });
  expect(proof.block).toEqual({
    repository_id: FROZEN_CUSTODY.repositoryId, workflow: FROZEN_CUSTODY.workflow, environment: FROZEN_CUSTODY.environment,
    run_id: RUN_ID, run_attempt: 1, source_sha: SOURCE_SHA, actor: SCHEMA_RELEASE.dispatcher.login,
    triggering_actor: SCHEMA_RELEASE.dispatcher.login, plan_run: PLAN_RUN, approval_sha256: expect.stringMatching(/^[0-9a-f]{64}$/),
    action: "seal", ci_conclusion: "success",
  });
  expect(Object.isFrozen(proof) && Object.isFrozen(proof.block)).toBe(true);
  for (const part of ["/git/ref/heads/main", `/actions/runs/${RUN_ID}`, `/actions/runs/${PLAN_RUN}`, "/environments/", "/approvals", "/deployments?", "/statuses"])
    expect(get.calls.some((c) => c.includes(part))).toBe(true);
});

test("no supervisor: outside Actions every job refuses before any GitHub read; SEALED is impossible", async () => {
  for (const action of ["copy", "readback", "seal"] as const) {
    const get = fakeGithub({}, action);
    expect(await refusal(verifySupervisor({ CUSTODY_MODE: action }, get, fixtureFacts(action)))).toEqual({ code: "frozen_copy_supervisor_missing", check: "actions" });
    expect(get.calls).toEqual([]);
  }
  expect(supervisorContextRefusal({}, "seal")?.code).toBe("frozen_copy_supervisor_missing");
  const seal = { manifestSha256: "e".repeat(64), inserted: true, orgs: 1, records: "1", deletions: "0", resets: "0", controlHeads: "0" };
  expect(jobReceipt("seal", undefined, testEnvelope(), seal)).toMatchObject({ status: "REFUSED", code: "frozen_copy_supervisor_missing", supervisor: null });
});

test("each forged Actions context field refuses with its own code and check", async () => {
  const cases: Array<[Record<string, string | undefined>, string, string]> = [
    [{ GITHUB_TOKEN: undefined }, "missing", "token"],
    [{ GITHUB_RUN_ID: "x" }, "missing", "run_id"],
    [{ CUSTODY_PLAN_RUN: "" }, "missing", "plan_run"],
    [{ CUSTODY_APPROVAL: "" }, "missing", "approval"],
    [{ GITHUB_REPOSITORY_ID: "1" }, "unqualified", "repository"],
    [{ GITHUB_REPOSITORY: "someone/fork" }, "unqualified", "repository"],
    [{ GITHUB_EVENT_NAME: "push" }, "unqualified", "event"],
    [{ GITHUB_REF: "refs/heads/feature" }, "unqualified", "ref"],
    [{ GITHUB_WORKFLOW_REF: `${FROZEN_CUSTODY.repository}/.github/workflows/other.yml@refs/heads/main` }, "unqualified", "workflow_ref"],
    [{ GITHUB_RUN_ATTEMPT: "2" }, "unqualified", "run_attempt"],
    [{ GITHUB_ACTOR: "someone-else" }, "unqualified", "actor"],
    [{ GITHUB_TRIGGERING_ACTOR: "someone-else" }, "unqualified", "triggering_actor"],
    [{ GITHUB_SHA: "6".repeat(40) }, "mismatch", "sha"],
  ];
  for (const [env, kind, check] of cases)
    expect(await refusal(attest("copy", { env }))).toEqual({ code: `frozen_copy_supervisor_${kind}`, check });
  // The mode input must equal the action this operator run performs.
  expect(await refusal(verifySupervisor(supervisorEnv("copy", fixtureApproval(fixtureFacts("copy"))), fakeGithub(), fixtureFacts("readback"))))
    .toEqual({ code: "frozen_copy_supervisor_mismatch", check: "mode" });
});

test("each forged GitHub answer refuses with its own code and check", async () => {
  const old = new Date(Date.now() - FROZEN_CUSTODY.planMaxAgeMs - 60_000).toISOString();
  const cases: Array<[Forge, string, string]> = [
    [{ main: () => ({ object: { sha: "6".repeat(40) } }) }, "stale", "main_moved"],
    [{ run: (b) => ({ ...b, repository: { id: 1 } }) }, "unqualified", "run"],
    [{ run: (b) => ({ ...b, path: ".github/workflows/other.yml" }) }, "unqualified", "run"],
    [{ run: (b) => ({ ...b, run_attempt: 2 }) }, "unqualified", "run"],
    [{ run: (b) => ({ ...b, actor: { ...b.actor, id: 1 } }) }, "unqualified", "run"],
    [{ run: (b) => ({ ...b, triggering_actor: { ...b.triggering_actor, type: "Bot" } }) }, "unqualified", "run"],
    [{ run: (b) => ({ ...b, head_sha: "6".repeat(40) }) }, "mismatch", "run_sha"],
    [{ run: (b) => ({ ...b, display_title: `frozen-source-custody seal ${SOURCE_SHA}` }) }, "mismatch", "run_mode"],
    [{ environment: () => { throw Object.assign(new Error("404"), { code: "release_github_api" }); } }, "missing", "github_api"],
    [{ environment: (b) => ({ ...b, protection_rules: null }) }, "unqualified", "environment"],
    [{ environment: (b) => ({ ...b, protection_rules: [{ type: "required_reviewers", reviewers: [{ type: "Team", reviewer: { id: 1, type: "Team" } }] }] }) }, "unqualified", "environment"],
    [{ environment: (b) => ({ ...b, deployment_branch_policy: { protected_branches: true, custom_branch_policies: false } }) }, "unqualified", "environment"],
    [{ environment: (b) => ({ ...b, name: "other" }) }, "unqualified", "environment"],
    [{ branches: (b) => ({ ...b, total_count: 2 }) }, "unqualified", "environment"],
    [{ ciRuns: (b) => ({ workflow_runs: [{ ...b.workflow_runs[0], conclusion: "failure" }] }) }, "unqualified", "ci"],
    [{ ciJobs: (b) => ({ ...b, jobs: [{ ...b.jobs[0], conclusion: "failure" }] }) }, "unqualified", "ci"],
    [{ plan: (b) => ({ ...b, conclusion: "failure" }) }, "unqualified", "plan_run"],
    [{ plan: (b) => ({ ...b, display_title: `frozen-source-custody copy ${SOURCE_SHA}` }) }, "unqualified", "plan_run"],
    [{ plan: (b) => ({ ...b, run_attempt: 2 }) }, "unqualified", "plan_run"],
    [{ plan: (b) => ({ ...b, head_sha: "6".repeat(40), display_title: `frozen-source-custody plan ${"6".repeat(40)}` }) }, "mismatch", "plan_sha"],
    [{ plan: (b) => ({ ...b, created_at: old }) }, "stale", "plan_age"],
    [{ approvals: () => [] }, "unqualified", "human_approval"],
    [{ approvals: (b) => [...b, { ...b[0], state: "rejected" }] }, "unqualified", "human_approval"],
    [{ approvals: (b) => [{ ...b[0], user: { login: "someone-else", id: 1, type: "User" } }] }, "unqualified", "human_approval"],
    [{ deployments: () => [] }, "unqualified", "environment_deployment"],
    [{ statuses: (b) => [{ ...b[0], log_url: `https://github.com/${FROZEN_CUSTODY.repository}/actions/runs/1` }] }, "unqualified", "environment_deployment"],
    [{ statuses: (b) => [{ ...b[0], state: "success" }] }, "unqualified", "environment_deployment"],
  ];
  for (const [forge, kind, check] of cases)
    expect({ forge: Object.keys(forge)[0], r: await refusal(attest("copy", { forge })) }).toEqual({ forge: Object.keys(forge)[0], r: { code: `frozen_copy_supervisor_${kind}`, check } });
});

test("CI gate: the full default push proof (ci.yml checks + session-read-worker / worker, pg-proof.yml PostgreSQL 15/16/17)", async () => {
  // (a) Every default run and job green: the GitHub half passes and reads both workflows and both job listings.
  const get = fakeGithub();
  expect(await verifyCustodyGithub(supervisorEnv("copy", fixtureApproval(fixtureFacts("copy"))), get, { protectedJob: true })).toEqual({ planRun: PLAN_RUN });
  for (const part of ["/actions/workflows/ci.yml/runs?", "/actions/workflows/pg-proof.yml/runs?", "/actions/runs/6001/attempts/1/jobs", "/actions/runs/6002/attempts/1/jobs"])
    expect(get.calls.some((c) => c.includes(part))).toBe(true);
  const worker = "session-read-worker / worker", pg16 = "real PostgreSQL 16";
  const job = (name: string, over: Record<string, unknown>) => (b: any) => ({ ...b, jobs: b.jobs.map((j: any) => (j.name === name ? { ...j, ...over } : j)) });
  const cases: Array<[string, Forge]> = [
    // (b) ci.yml worker job missing (listing stays self-consistent so only the missing name can refuse).
    ["worker missing", { ciJobs: (b) => ({ total_count: b.jobs.length - 1, jobs: b.jobs.filter((j: any) => j.name !== worker) }) }],
    // (c) worker job still running; (d) worker job failed.
    ["worker in_progress", { ciJobs: job(worker, { status: "in_progress", conclusion: null }) }],
    ["worker failed", { ciJobs: job(worker, { conclusion: "failure" }) }],
    // (e) no pg-proof.yml push run at the sha.
    ["pg-proof missing", { pgRuns: () => ({ workflow_runs: [] }) }],
    // (f) pg-proof PostgreSQL 16 failed; (g) pg-proof run still running.
    ["pg16 failed", { pgJobs: job(pg16, { conclusion: "failure" }) }],
    ["pg-proof in_progress", { pgRuns: (b) => ({ workflow_runs: [{ ...b.workflow_runs[0], status: "in_progress", conclusion: null }] }) }],
  ];
  for (const [name, forge] of cases) {
    const g = fakeGithub(forge);
    expect({ name, r: await refusal(attest("copy", { forge })) }).toEqual({ name, r: { code: "frozen_copy_supervisor_unqualified", check: "ci" } });
    expect({ name, r: await refusal(verifyCustodyGithub(supervisorEnv("copy", fixtureApproval(fixtureFacts("copy"))), g, { protectedJob: false })) })
      .toEqual({ name, r: { code: "frozen_copy_supervisor_unqualified", check: "ci" } });
    // Refused at the CI step: nothing after it (plan run, approvals, deployments) is read.
    expect({ name, later: g.calls.some((c) => c.includes(`/actions/runs/${PLAN_RUN}`) || c.includes("/approvals") || c.includes("/deployments")) })
      .toEqual({ name, later: false });
  }
});

test("each approval field is bound: a change anywhere refuses mismatch naming that field", async () => {
  const f = fixtureFacts("copy");
  const good = fixtureApproval(f);
  const swap = (label: string, value: string) => good.replace(new RegExp(`${label} \\S+`), `${label} ${value}`);
  const cases: Array<[string, string]> = [
    [good.replace("FROZEN-COPY COPY", "FROZEN-COPY SEAL"), "approval_action"],
    [good.replace(SOURCE_SHA, "6".repeat(40)), "approval_sha"],
    [swap("SOURCE", "1".repeat(64)), "approval_source"],
    [swap("TARGET", "1".repeat(64)), "approval_target"],
    [swap("EPOCH", "1".repeat(64)), "approval_epoch"],
    [swap("PRIVACY", "1".repeat(64)), "approval_privacy"],
    [swap("GENERATION", "1".repeat(64)), "approval_generation"],
    [swap("MANIFEST", "1".repeat(64)), "approval_manifest"],
    [swap("OPERATOR", "1".repeat(64)), "approval_operator"],
    [swap("PRODUCER", "1".repeat(64)), "approval_producer"],
    [swap("RECEIPT", "frozen-source-copy-job-v2"), "approval_receipt"],
    [swap("PAGE", "1000"), "approval_page"],
    [swap("MAXPAGES", "9"), "approval_maxpages"],
    [swap("BUDGET", "1"), "approval_budget"],
    [swap("PLAN", "6999"), "approval_plan"],
    [good + " EXTRA", "approval_format"],
    [good.replace(/ /g, "  "), "approval_action"],
  ];
  for (const [approval, check] of cases)
    expect({ check, r: await refusal(attest("copy", { approval })) }).toEqual({ check, r: { code: "frozen_copy_supervisor_mismatch", check } });
  // The live facts differing from a correct-looking approval refuse the same way (the operator derives them itself).
  expect(await refusal(attest("copy", { facts: { epoch: "1".repeat(64) } }))).toEqual({ code: "frozen_copy_supervisor_mismatch", check: "approval_epoch" });
  // A seal approval without a 64-hex MANIFEST, or a copy approval naming one, refuses.
  expect(await refusal(attest("seal", { approval: fixtureApproval(fixtureFacts("seal")) }))).toEqual({ code: "frozen_copy_supervisor_mismatch", check: "approval_manifest" });
  // Approval checks run before any GitHub read.
  const get = fakeGithub();
  await refusal(verifySupervisor(supervisorEnv("copy", swap("EPOCH", "1".repeat(64))), get, f));
  expect(get.calls).toEqual([]);
});

test("brand: only verifySupervisor mints a proof; copies, JSON and foreign symbols are rejected", async () => {
  const proof = await mintSupervisorProof("seal", { manifest: "e".repeat(64) });
  const forged = [
    { ...proof }, JSON.parse(JSON.stringify(proof)), Object.create(proof), { action: "seal", block: proof.block },
    { [Symbol("frozen-copy-supervisor-proof")]: true, action: "seal", block: proof.block }, null, "seal", 1,
  ];
  for (const x of forged) {
    expect(isSupervisorProof(x)).toBe(false);
    expect(supervisorProofRefusal(x, "seal")?.code).toBe("frozen_copy_supervisor_missing");
  }
  expect(supervisorProofRefusal(proof, "seal")).toBeNull();
  expect(supervisorProofRefusal(proof, "copy")?.code).toBe("frozen_copy_supervisor_mismatch");
  // The proof is frozen: its action cannot be rewritten in place.
  expect(() => { (proof as any).action = "copy"; }).toThrow();
});

test("the receipt's supervisor block is sanitized", async () => {
  const proof = await mintSupervisorProof("copy");
  const text = JSON.stringify(jobReceipt("copy", undefined, testEnvelope(), null, { supervisor: proof }).supervisor);
  expect(text).not.toMatch(/[a-z]+:\/\//i);
  expect(text).not.toMatch(/ep-|postgres\.|@|token|password/i);
  expect(text).not.toContain("synthetic-github-token");
  const allowed = new Set(["source_sha", "approval_sha256"]);
  for (const [k, v] of Object.entries(JSON.parse(text))) if (!allowed.has(k)) expect(String(v)).not.toMatch(/[0-9a-f]{32,}/);
});

test("constants are pinned to the workflow YAML, the doc and the release contracts", () => {
  expect(FROZEN_CUSTODY.repositoryId).toBe(PRIVATE_INGEST_RELEASE.repositoryId);
  expect(FROZEN_CUSTODY.repository).toBe(SCHEMA_RELEASE.repository);
  expect(FROZEN_CUSTODY.dispatcher).toEqual(SCHEMA_RELEASE.dispatcher);
  expect(FROZEN_COPY_RECEIPT_SCHEMA).toBe("frozen-source-copy-job-v3");
  expect(FROZEN_COPY_CREDENTIAL_PRODUCER_SHA256).toMatch(/^[0-9a-f]{64}$/);
  // The operator step is exactly the producer constant, once; the secret appears only in that job's step env.
  expect(yaml).toContain(`        run: ${FROZEN_COPY_CREDENTIAL_PRODUCER}\n`);
  expect(yaml.split(FROZEN_COPY_CREDENTIAL_PRODUCER).length).toBe(2);
  expect(yaml.match(/secrets\.[A-Z_]+/g)).toEqual(["secrets.FROZEN_COPY_CREDENTIALS"]);
  expect(yaml).toContain(`run-name: frozen-source-custody \${{ inputs.mode }} \${{ inputs.sha }}`);
  expect(yaml).toContain("options: [plan, copy, seal, readback]");
  expect(yaml).toContain(`if: github.repository_id == '${FROZEN_CUSTODY.repositoryId}' && github.ref == 'refs/heads/main'`);
  expect(yaml).toContain(`echo "environment=${FROZEN_CUSTODY.environment}" >> "$GITHUB_OUTPUT"`);
  expect(yaml).toContain("    environment: ${{ needs.prerequisite-gate.outputs.environment }}");
  expect(yaml).toContain("permissions:\n  contents: read\n  actions: read\n  deployments: read\nconcurrency:\n");
  expect(yaml).toContain("persist-credentials: false");
  expect(yaml).toContain("bun scripts/frozen-source-custody-github.ts --protected");
  // Inputs reach the shell only through env (no expression interpolation inside a run script).
  expect(yaml).not.toMatch(/run:[^\n]*\$\{\{/);
  expect(yaml.split("run: |")[1]?.split("\n").slice(1, 3).join("\n")).not.toContain("${{");
  expect(yaml).not.toMatch(/^on:[\s\S]*?^\s+(push|pull_request|schedule|workflow_run):/m);
  expect(yaml.match(/^  [a-z-]+:$/gm)?.filter((j) => /prerequisite-gate|custody/.test(j))).toEqual(["  prerequisite-gate:", "  custody:"]);
  for (const s of [FROZEN_CUSTODY.workflow, FROZEN_CUSTODY.environment, "Khaliq's actions only", "FROZEN_COPY_CREDENTIALS", FROZEN_COPY_CREDENTIAL_PRODUCER,
    "frozen_copy_supervisor_missing", "frozen_copy_supervisor_unqualified", "frozen_copy_supervisor_stale", "frozen_copy_supervisor_mismatch"])
    expect(doc).toContain(s);
});

test("gate script: ok for the prerequisite and protected gates; fixed code and check on a forged answer", async () => {
  expect(await frozenSourceCustodyGate(supervisorEnv("plan"), fakeGithub({}, "plan"), false)).toEqual({ ok: true });
  const approval = fixtureApproval(fixtureFacts("copy"));
  expect(await frozenSourceCustodyGate(supervisorEnv("copy", approval), fakeGithub(), true)).toEqual({ ok: true });
  // The prerequisite gate (no environment yet) does not read approvals or deployments.
  const pre = fakeGithub();
  expect(await frozenSourceCustodyGate(supervisorEnv("copy", approval), pre, false)).toEqual({ ok: true });
  expect(pre.calls.some((c) => c.includes("/approvals") || c.includes("/deployments"))).toBe(false);
  expect(await frozenSourceCustodyGate(supervisorEnv("copy", approval), fakeGithub({ approvals: () => [] }), true))
    .toEqual({ ok: false, code: "frozen_copy_supervisor_unqualified", check: "human_approval" });
  expect(await frozenSourceCustodyGate({}, fakeGithub(), false)).toEqual({ ok: false, code: "frozen_copy_supervisor_missing", check: "actions" });
  expect(await frozenSourceCustodyGate(supervisorEnv("copy", approval, { CUSTODY_MODE: "repair-head" }), fakeGithub(), false))
    .toEqual({ ok: false, code: "frozen_copy_supervisor_mismatch", check: "mode" });
  // A non-refusal failure is reported as missing/github_api, never with its message.
  expect(await frozenSourceCustodyGate(supervisorEnv("copy", approval), async () => { throw new Error("secret detail"); }, false))
    .toEqual({ ok: false, code: "frozen_copy_supervisor_missing", check: "github_api" });
});

test("gate script process: exit 0 on the synthetic GitHub, exit 1 with a fixed line on a forged route", async () => {
  const preload = fileURLToPath(new URL("./fixtures/frozen-copy-preload.ts", import.meta.url));
  const run = async (forge: string | null, extra: string[]) => {
    const env = { PATH: process.env.PATH!, FROZEN_COPY_TEST_GITHUB: "1", ...supervisorEnv("copy", fixtureApproval(fixtureFacts("copy"))) as Record<string, string>,
      ...(forge ? { FROZEN_COPY_TEST_GITHUB_FORGE: forge } : {}) };
    const child = Bun.spawn([process.execPath, "--preload", preload, root("scripts/frozen-source-custody-github.ts"), ...extra], { stdout: "pipe", stderr: "pipe", env, timeout: 60000 });
    const [out, err, code] = [await new Response(child.stdout).text(), await new Response(child.stderr).text(), await child.exited];
    // Drop the preload's synthetic proof summary (printed at exit); the gate's own output is one fixed line.
    return { code, text: (out + "\n" + err).split("\n").filter((l) => l.trim() && !l.startsWith('{"syntheticCopyProof"')).join("\n") };
  };
  expect(await run(null, ["--protected"])).toEqual({ code: 0, text: "frozen_source_custody_gate_ok" });
  expect(await run("deployments", ["--protected"])).toEqual({ code: 1, text: "frozen_source_custody_gate_refused frozen_copy_supervisor_unqualified environment_deployment" });
  expect(await run("main", [])).toEqual({ code: 1, text: "frozen_source_custody_gate_refused frozen_copy_supervisor_stale main_moved" });
}, 120000);
