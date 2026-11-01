/**
 * The second hardcoded proof target: the Pippen Tools sandbox of agent
 * aws-proof (account 361835732085, us-west-2; Khaliq, 2026-11-01). Every check
 * that pins the sessions-dev proof target pins this one with equal strictness,
 * and the sessions-dev target behaves exactly as before.
 */
import { describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { newCtx, postDestroyTeardown } from "../checks.ts";
import { observeIdentity, parseArgs } from "../cli.ts";
import { clusterArnOf, parseDescribeCluster, proofKubeconfig } from "../cluster-binding.ts";
import { defaultConfig, defaultInputsPath, loadConfig, sandboxConfig, validateConfig } from "../config.ts";
import { Runner, type Spawn } from "../exec.ts";
import {
  fenceFor, PROOF_BACKUP_BUCKET, PROOF_CONTEXT, PROOF_FENCE, PROOF_HOST, PROOF_LOCKER_BUCKET, proofConfigIssues, SANDBOX_BACKUP_BUCKET, SANDBOX_CONTEXT, SANDBOX_FENCE, SANDBOX_HOST, SANDBOX_LOCKER_BUCKET, TARGET_VALUES,
} from "../fence.ts";
import {
  assertMayRun, assertPostDestroyTarget, assertProofTarget, GuardError, PIPPEN_SANDBOX_PROOF_TARGET, PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_STAGE, PROOF_TARGETS, PIPPEN_SESSION_ID, SANDBOX_ACCOUNT_ID, SANDBOX_CLUSTER, sandboxCallerArn, SANDBOX_STAGE, SESSIONS_DEV_PROOF_TARGET,
  type GuardInput, type ProofTarget,
} from "../guard.ts";
import { PROOF_TAG, SANDBOX_DESTROY_COMMAND, STAGE_DESTROY_COMMAND, stageDestroyCommandFor, teardownVerifySteps, withoutExempt } from "../lifecycle.ts";
import { stepFenceIssue } from "../ops.ts";
import { loadStagePlan, sha256Hex, STAGE_PLAN_FILES, stagePlanFiles } from "../provenance.ts";
import { admitPrior, buildReport, canonicalReportBytes } from "../report.ts";
import { PROV } from "./prov.ts";

const SBX = PIPPEN_SANDBOX_PROOF_TARGET;
/** A pippen lease id as `pippen sandbox status aws-proof` shows it (crypto.randomUUID). */
const SESSION = "3f2b8c1e-9d4a-4e6b-a1c7-5b0e2f9d8a14";
const SANDBOX_CALLER_ARN = `arn:aws:sts::361835732085:assumed-role/agent-session/agent-${SESSION}`;
const ok: GuardInput = { mode: "live", confirmLive: true, mutating: true, accountId: SANDBOX_ACCOUNT_ID, callerArn: SANDBOX_CALLER_ARN, pippenSessionId: SESSION, clusterName: SANDBOX_CLUSTER, kubeContext: SANDBOX_CONTEXT, kubeContextCluster: SANDBOX_CONTEXT, clusterBound: true };
const devOk: GuardInput = { mode: "live", confirmLive: true, mutating: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: PROOF_CONTEXT, kubeContextCluster: PROOF_CONTEXT, clusterBound: true };

const code = (f: () => void) => {
  try {
    f();
    return "allowed";
  } catch (e) {
    return e instanceof GuardError ? e.code : `other: ${(e as Error).message}`;
  }
};

describe("pippen sandbox target: the constants are exact", () => {
  test("account, region, stage, cluster, context, caller and teardown are pinned", () => {
    expect(SBX).toMatchObject({ id: "pippen-sandbox", accountId: "361835732085", region: "us-west-2", stage: "aws-proof-sandbox", clusterName: "96bulls-aws-proof-sandbox", destroyCommand: "pippen sandbox down aws-proof" });
    expect(sandboxCallerArn(SESSION)).toBe(SANDBOX_CALLER_ARN);
    expect(SBX.callerRolePrefix).toBe("arn:aws:sts::361835732085:assumed-role/agent-session/");
    expect(SANDBOX_CONTEXT).toBe("arn:aws:eks:us-west-2:361835732085:cluster/96bulls-aws-proof-sandbox");
    expect(SANDBOX_LOCKER_BUCKET).toBe("96bulls-aws-proof-sandbox-locker-361835732085");
    expect(SANDBOX_BACKUP_BUCKET).toBe("96bulls-aws-proof-sandbox-hoopdb-backups-361835732085");
    expect(SBX.sweepTags).toEqual([{ key: "alchemy::stage", value: "aws-proof-sandbox" }, { key: "pippen:sandbox", value: "true" }]);
    expect(SBX.residueExemptArns).toEqual(["arn:aws:iam::361835732085:role/pippen/agent-session"]);
    expect(Object.isFrozen(SBX) && Object.isFrozen(SESSIONS_DEV_PROOF_TARGET) && Object.isFrozen(PROOF_TARGETS)).toBe(true);
    expect(Object.keys(PROOF_TARGETS).sort()).toEqual(["pippen-sandbox", "proof"]);
  });
  test("the stack's pin agrees with the harness's", async () => {
    const { PIPPEN_PROOF_SANDBOX, pippenProofSandboxCallerArn } = await import("../../../../stacks/aws/sandbox-rules.ts");
    expect(PIPPEN_PROOF_SANDBOX).toMatchObject({ stage: SBX.stage, accountId: SBX.accountId, region: SBX.region, teardownCommand: SBX.destroyCommand });
    expect(pippenProofSandboxCallerArn({ NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION })).toBe(SANDBOX_CALLER_ARN);
  });
});

describe("pippen sandbox target: guard", () => {
  const run = (g: GuardInput, t: ProofTarget = SBX) => code(() => assertMayRun(g, t));
  test("accepts exactly the sandbox account + agent-session caller + cluster + context", () => {
    expect(run(ok)).toBe("allowed");
    expect(run({ ...ok, mode: "read-only", mutating: false })).toBe("allowed");
  });
  test("still needs --confirm-live", () => {
    expect(run({ ...ok, confirmLive: false })).toBe("missing_confirm_live");
  });
  test("refuses any other account, the sessions-dev proof account included", () => {
    expect(run({ ...ok, accountId: PROOF_ACCOUNT_ID })).toBe("wrong_account");
    expect(run({ ...ok, accountId: "904260146565" })).toBe("wrong_account"); // the org management account
    expect(run({ ...ok, accountId: "123456789012" })).toBe("wrong_account");
    expect(run({ ...ok, accountId: undefined })).toBe("unverified_identity");
  });
  test("refuses any caller but the pippen agent-session role session", () => {
    expect(run({ ...ok, callerArn: undefined })).toBe("unverified_identity");
    expect(run({ ...ok, callerArn: "arn:aws:sts::361835732085:assumed-role/OrganizationAccountAccessRole/agent-aws-proof" })).toBe("wrong_caller");
    expect(run({ ...ok, callerArn: "arn:aws:sts::361835732085:assumed-role/agent-session/agent-someone-else" })).toBe("wrong_caller");
    expect(run({ ...ok, callerArn: "arn:aws:iam::361835732085:user/agent-session" })).toBe("wrong_caller");
    expect(run({ ...ok, callerArn: "arn:aws:sts::500268966557:assumed-role/agent-session/agent-aws-proof" })).toBe("wrong_caller");
    expect(run({ ...ok, callerArn: `${SANDBOX_CALLER_ARN}x` })).toBe("wrong_caller");
  });
  test("live steps pin the caller to the given pippen session: right uuid only", () => {
    const other = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d";
    expect(run(ok)).toBe("allowed");
    // another lease's session, even of the right role
    expect(run({ ...ok, pippenSessionId: other })).toBe("wrong_caller");
    expect(run({ ...ok, callerArn: `arn:aws:sts::361835732085:assumed-role/agent-session/agent-${other}` })).toBe("wrong_caller");
    // the old fixed session name is not a lease id
    expect(run({ ...ok, callerArn: "arn:aws:sts::361835732085:assumed-role/agent-session/agent-aws-proof" })).toBe("wrong_caller");
    expect(run({ ...ok, pippenSessionId: "aws-proof", callerArn: "arn:aws:sts::361835732085:assumed-role/agent-session/agent-aws-proof" })).toBe("missing_pippen_session");
    // missing or malformed session id
    expect(run({ ...ok, pippenSessionId: undefined })).toBe("missing_pippen_session");
    for (const bad of [SESSION.toUpperCase(), SESSION.replace("-4e6b-", "-1e6b-"), SESSION.replace("-a1c7-", "-c1c7-"), SESSION.slice(1), `${SESSION} `, `${SESSION}\n`, "", "*"])
      expect(run({ ...ok, pippenSessionId: bad })).toBe("missing_pippen_session");
    expect(PIPPEN_SESSION_ID.test(SESSION)).toBe(true);
    expect(sandboxCallerArn("aws-proof")).toBeUndefined();
  });
  test("refuses any other cluster, the sessions-dev proof cluster included", () => {
    expect(run({ ...ok, clusterName: PROOF_CLUSTER })).toBe("wrong_cluster");
    expect(run({ ...ok, clusterName: "96bulls-aws-proof-sandbox2" })).toBe("wrong_cluster");
  });
  test("refuses a context in another account, cluster or region", () => {
    expect(run({ ...ok, kubeContextCluster: PROOF_CONTEXT })).toBe("context_mismatch");
    expect(run({ ...ok, kubeContextCluster: "arn:aws:eks:us-west-2:361835732085:cluster/other" })).toBe("context_mismatch");
    expect(run({ ...ok, kubeContextCluster: "arn:aws:eks:us-east-1:361835732085:cluster/96bulls-aws-proof-sandbox" })).toBe("wrong_region");
    expect(run({ ...ok, kubeContext: "arn:aws:eks:us-east-1:361835732085:cluster/96bulls-aws-proof-sandbox" })).toBe("wrong_region");
    expect(run({ ...ok, clusterBound: false })).toBe("cluster_unbound");
  });
  test("production-shaped names are refused here too", () => {
    expect(run({ ...ok, kubeContext: "sessions-dev" })).toBe("production_name");
  });
  test("a look-alike target object is refused: only the two frozen targets exist", () => {
    const fake = { ...SBX, accountId: "123456789012" } as ProofTarget;
    expect(code(() => assertProofTarget({ ...ok, accountId: "123456789012" }, fake))).toBe("wrong_account");
    expect(code(() => assertPostDestroyTarget({ confirmLive: true, accountId: "123456789012", clusterName: SBX.clusterName }, fake))).toBe("wrong_account");
  });
  test("post-destroy accepts a fresh agent-session session (any session name), live steps do not", () => {
    const fresh = "arn:aws:sts::361835732085:assumed-role/agent-session/pippen-agent-0b7d6c1e";
    const pd = { confirmLive: true, accountId: SANDBOX_ACCOUNT_ID, clusterName: SANDBOX_CLUSTER };
    expect(code(() => assertPostDestroyTarget({ ...pd, callerArn: fresh }, SBX))).toBe("allowed");
    expect(run({ ...ok, callerArn: fresh, postDestroy: true, mutating: false })).toBe("allowed");
    expect(run({ ...ok, callerArn: fresh })).toBe("wrong_caller");
    for (const bad of [
      "arn:aws:sts::361835732085:assumed-role/pippen-sweeper/x1",
      "arn:aws:sts::361835732085:assumed-role/agent-session-admin/x1",
      "arn:aws:sts::500268966557:assumed-role/agent-session/x1",
      "arn:aws:sts::361835732085:assumed-role/agent-session/",
      "arn:aws:sts::361835732085:assumed-role/agent-session/a/b",
      "arn:aws:iam::361835732085:role/pippen/agent-session",
    ])
      expect(code(() => assertPostDestroyTarget({ ...pd, callerArn: bad }, SBX))).toBe("wrong_caller");
    expect(code(() => assertPostDestroyTarget({ ...pd, callerArn: undefined }, SBX))).toBe("unverified_identity");
  });
  test("post-destroy: sandbox account + caller + cluster, reads only", () => {
    const pd = { confirmLive: true, accountId: SANDBOX_ACCOUNT_ID, callerArn: SANDBOX_CALLER_ARN, clusterName: SANDBOX_CLUSTER };
    expect(code(() => assertPostDestroyTarget(pd, SBX))).toBe("allowed");
    expect(code(() => assertPostDestroyTarget({ ...pd, accountId: PROOF_ACCOUNT_ID }, SBX))).toBe("wrong_account");
    expect(code(() => assertPostDestroyTarget({ ...pd, callerArn: undefined }, SBX))).toBe("unverified_identity");
    expect(code(() => assertPostDestroyTarget({ ...pd, callerArn: "arn:aws:sts::361835732085:assumed-role/admin/x" }, SBX))).toBe("wrong_caller");
    expect(code(() => assertPostDestroyTarget({ ...pd, clusterName: PROOF_CLUSTER }, SBX))).toBe("wrong_cluster");
    expect(run({ ...ok, postDestroy: true, mutating: true })).toBe("post_destroy_read_only");
  });
  test("the sessions-dev target refuses the sandbox identity, and the sandbox target refuses sessions-dev's", () => {
    expect(code(() => assertMayRun(ok))).toBe("wrong_account");
    expect(code(() => assertMayRun(devOk, SBX))).toBe("wrong_account");
  });
});

describe("sessions-dev proof target: unchanged", () => {
  test("defaults are the sessions-dev target, and its guard needs no caller ARN", () => {
    expect(SESSIONS_DEV_PROOF_TARGET).toMatchObject({ id: "proof", accountId: "500268966557", stage: "aws-proof", clusterName: "96bulls-aws-proof", region: "us-west-2" });
    expect(SESSIONS_DEV_PROOF_TARGET.callerArn).toBeUndefined();
    expect(code(() => assertMayRun(devOk))).toBe("allowed");
    expect(code(() => assertMayRun(devOk, SESSIONS_DEV_PROOF_TARGET))).toBe("allowed");
    expect(code(() => assertMayRun({ ...devOk, callerArn: "anything" }))).toBe("allowed");
  });
  test("fence, constants and commands are byte-for-byte what they were", () => {
    expect(fenceFor("proof")).toBe(PROOF_FENCE);
    expect(PROOF_CONTEXT).toBe("arn:aws:eks:us-west-2:500268966557:cluster/96bulls-aws-proof");
    expect(PROOF_HOST).toBe("proof-sessions.agentrelay.com");
    expect([...PROOF_FENCE.buckets]).toEqual(["96bulls-aws-proof-locker-500268966557", "96bulls-aws-proof-hoopdb-backups-500268966557"]);
    expect([...PROOF_FENCE.hosts]).toEqual(["127.0.0.1", "proof-sessions.agentrelay.com", "agentworkforce.github.io"]);
    expect(PROOF_FENCE.accountId).toBe("500268966557");
    expect(PROOF_TAG).toEqual({ key: "96bulls:stack", value: "aws-proof" });
    expect(STAGE_DESTROY_COMMAND).toBe("bin/96bulls down aws-proof   # Alchemy destroy; requires Khaliq's OK; not run by this harness");
    expect(stageDestroyCommandFor(PROOF_STAGE)).toBe(STAGE_DESTROY_COMMAND);
    expect(teardownVerifySteps(defaultConfig()).map((s) => s.argv.join(" "))).toEqual([
      "aws eks list-clusters --region us-west-2 --query clusters --output json",
      `kubectl --context ${PROOF_CONTEXT} -n 96bulls get all,pvc,backups.postgresql.cnpg.io,clusters.postgresql.cnpg.io -l 96bulls.proof=true -o json`,
      `kubectl --context ${PROOF_CONTEXT} -n relaycast get all,pvc,backups.postgresql.cnpg.io,clusters.postgresql.cnpg.io -l 96bulls.proof=true -o json`,
      `kubectl --context ${PROOF_CONTEXT} get namespace 96bulls --ignore-not-found -o name`,
      "aws resourcegroupstaggingapi get-resources --region us-west-2 --tag-filters Key=96bulls:stack,Values=aws-proof --output json",
    ]);
    expect(STAGE_PLAN_FILES).toEqual({ provenance: "docs/plans/aws-proof.provenance.json", plan: "docs/plans/aws-proof.plan.txt", inputs: "docs/plans/aws-proof.inputs.sanitized.json" });
    expect(stagePlanFiles(PROOF_STAGE)).toBe(STAGE_PLAN_FILES);
    expect(defaultInputsPath("proof")).toBe(".96bulls/aws-proof/inputs.json");
  });
  test("observeIdentity runs exactly the same four reads, and never asks for the caller ARN", () => {
    const calls: string[][] = [];
    const run = (argv: string[]) => {
      calls.push(argv);
      return argv[1] === "sts" ? { stdout: `${PROOF_ACCOUNT_ID}\n`, status: 0 } : { stdout: "", status: 1 };
    };
    observeIdentity(defaultConfig(), run);
    expect(calls).toEqual([
      ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"],
      ["aws", "eks", "describe-cluster", "--name", "96bulls-aws-proof", "--region", "us-west-2", "--output", "json"],
    ]);
  });
  test("the sessions-dev fence still refuses everything sandbox-shaped", () => {
    expect(stepFenceIssue(["kubectl", "--context", SANDBOX_CONTEXT, "-n", "96bulls", "get", "pods"], PROOF_FENCE)).toBe("context_outside_fence");
    expect(stepFenceIssue(["aws", "eks", "describe-cluster", "--name", SANDBOX_CLUSTER, "--region", "us-west-2"], PROOF_FENCE)).toBe("aws_operation_not_allowlisted");
    expect(stepFenceIssue(["aws", "resourcegroupstaggingapi", "get-resources", "--region", "us-west-2", "--tag-filters", "Key=pippen:sandbox,Values=true"], PROOF_FENCE)).toBe("aws_operation_not_allowlisted");
    expect(stepFenceIssue(["aws", "s3api", "head-bucket", "--bucket", SANDBOX_LOCKER_BUCKET, "--region", "us-west-2"], PROOF_FENCE)).toBe("bucket_outside_fence");
  });
});

describe("pippen sandbox target: fence", () => {
  const k = (...a: string[]) => ["kubectl", "--context", SANDBOX_CONTEXT, ...a];
  test("admits the sandbox context, cluster, buckets, host and sweeper tag", () => {
    expect(fenceFor("pippen-sandbox")).toBe(SANDBOX_FENCE);
    expect(stepFenceIssue(k("-n", "96bulls", "get", "pods"), SANDBOX_FENCE)).toBeUndefined();
    expect(stepFenceIssue(["helm", "--kube-context", SANDBOX_CONTEXT, "-n", "96bulls", "list", "-o", "json"], SANDBOX_FENCE)).toBeUndefined();
    expect(stepFenceIssue(["aws", "eks", "describe-cluster", "--name", SANDBOX_CLUSTER, "--region", "us-west-2"], SANDBOX_FENCE)).toBeUndefined();
    expect(stepFenceIssue(["aws", "resourcegroupstaggingapi", "get-resources", "--region", "us-west-2", "--tag-filters", "Key=96bulls:stack,Values=aws-proof-sandbox"], SANDBOX_FENCE)).toBeUndefined();
    expect(stepFenceIssue(["aws", "resourcegroupstaggingapi", "get-resources", "--region", "us-west-2", "--tag-filters", "Key=pippen:sandbox,Values=true"], SANDBOX_FENCE)).toBeUndefined();
    for (const b of [SANDBOX_LOCKER_BUCKET, SANDBOX_BACKUP_BUCKET]) expect(stepFenceIssue(["aws", "s3api", "head-bucket", "--bucket", b, "--region", "us-west-2"], SANDBOX_FENCE)).toBeUndefined();
    expect(stepFenceIssue(["aws", "kms", "describe-key", "--region", "us-west-2", "--key-id", "arn:aws:kms:us-west-2:361835732085:key/abc"], SANDBOX_FENCE)).toBeUndefined();
    expect(stepFenceIssue(["HTTP", "GET", `https://${SANDBOX_HOST}/healthz`], SANDBOX_FENCE)).toBeUndefined();
  });
  test("refuses the sessions-dev context, cluster, buckets, ARNs, host and stage tag", () => {
    expect(stepFenceIssue(["kubectl", "--context", PROOF_CONTEXT, "-n", "96bulls", "get", "pods"], SANDBOX_FENCE)).toBe("context_outside_fence");
    expect(stepFenceIssue(["aws", "eks", "describe-cluster", "--name", PROOF_CLUSTER, "--region", "us-west-2"], SANDBOX_FENCE)).toBe("aws_operation_not_allowlisted");
    expect(stepFenceIssue(["aws", "s3api", "head-bucket", "--bucket", PROOF_LOCKER_BUCKET, "--region", "us-west-2"], SANDBOX_FENCE)).toBe("bucket_outside_fence");
    expect(stepFenceIssue(["aws", "s3api", "head-bucket", "--bucket", PROOF_BACKUP_BUCKET, "--region", "us-west-2"], SANDBOX_FENCE)).toBe("bucket_outside_fence");
    expect(stepFenceIssue(["aws", "kms", "describe-key", "--region", "us-west-2", "--key-id", "arn:aws:kms:us-west-2:500268966557:key/abc"], SANDBOX_FENCE)).toBe("arn_outside_fence");
    expect(stepFenceIssue(["aws", "resourcegroupstaggingapi", "get-resources", "--region", "us-west-2", "--tag-filters", "Key=96bulls:stack,Values=aws-proof"], SANDBOX_FENCE)).toBe("aws_operation_not_allowlisted");
    expect(stepFenceIssue(["aws", "resourcegroupstaggingapi", "get-resources", "--region", "us-west-2", "--tag-filters", "Key=pippen:sandbox,Values=false"], SANDBOX_FENCE)).toBe("aws_operation_not_allowlisted");
    expect(stepFenceIssue(["aws", "resourcegroupstaggingapi", "get-resources", "--region", "us-west-2", "--tag-filters", "Key=alchemy::stage,Values=aws-proof"], SANDBOX_FENCE)).toBe("aws_operation_not_allowlisted");
    expect(stepFenceIssue(["aws", "resourcegroupstaggingapi", "get-resources", "--region", "us-west-2", "--tag-filters", "Key=alchemy::stage,Values=aws-proof-sandbox"], PROOF_FENCE)).toBe("aws_operation_not_allowlisted");
    expect(stepFenceIssue(["aws", "eks", "list-clusters", "--region", "us-east-1"], SANDBOX_FENCE)).toBe("region_outside_fence");
    expect(stepFenceIssue(["HTTP", "GET", `https://${PROOF_HOST}/healthz`], SANDBOX_FENCE)).toBe("host_outside_fence");
  });
  test("a fence whose account or target was tampered with admits no aws step", () => {
    const describe = ["aws", "eks", "list-clusters", "--region", "us-west-2"];
    expect(stepFenceIssue(describe, { ...SANDBOX_FENCE, accountId: PROOF_ACCOUNT_ID })).toBe("aws_outside_fence");
    expect(stepFenceIssue(describe, { ...SANDBOX_FENCE, proofTarget: { ...SBX } as ProofTarget })).toBe("aws_outside_fence");
    expect(stepFenceIssue(describe, { ...PROOF_FENCE, proofTarget: undefined })).toBe("aws_outside_fence");
  });
});

describe("pippen sandbox target: runner", () => {
  const spy = () => {
    const calls: string[][] = [];
    const spawn: Spawn = (argv) => (calls.push(argv), { stdout: "{}", status: 0 });
    return { calls, spawn };
  };
  const step = { id: "x", argv: ["kubectl", "--context", SANDBOX_CONTEXT, "-n", "96bulls", "get", "pods", "-o", "json"], mutating: false, touches: [], approval: "operator" as const };
  const runner = (identity: Omit<GuardInput, "mode" | "mutating">, spawn: Spawn) => new Runner({ mode: "live", identity, fence: SANDBOX_FENCE, kubeconfigPath: "/test/k.json", spawn, print: () => {} });
  test("runs only with the exact sandbox identity", () => {
    const s = spy();
    expect(runner({ ...ok }, s.spawn).run(step).record.status).toBe("ran");
    expect(s.calls).toHaveLength(1);
  });
  test("a wrong caller, account or missing ARN is refused before anything spawns", () => {
    const s = spy();
    expect(runner({ ...ok, callerArn: "arn:aws:sts::361835732085:assumed-role/admin/me" }, s.spawn).run(step).record.code).toBe("wrong_caller");
    expect(runner({ ...ok, callerArn: undefined }, s.spawn).run(step).record.code).toBe("unverified_identity");
    expect(runner({ ...ok, accountId: PROOF_ACCOUNT_ID }, s.spawn).run(step).record.code).toBe("wrong_account");
    expect(s.calls).toHaveLength(0);
  });
});

describe("pippen sandbox target: cluster binding and observed identity", () => {
  const described = (arn: string, name: string) => JSON.stringify({ cluster: { arn, name, endpoint: "https://ABC123.gr7.us-west-2.eks.amazonaws.com", status: "ACTIVE", createdAt: "2026-11-01T00:00:00Z", certificateAuthority: { data: "Q0E=" } } });
  test("describe-cluster must be the sandbox cluster in the sandbox account", () => {
    expect(clusterArnOf(SBX)).toBe(SANDBOX_CONTEXT);
    expect(parseDescribeCluster(described(SANDBOX_CONTEXT, SANDBOX_CLUSTER), SBX)).toMatchObject({ arn: SANDBOX_CONTEXT });
    expect(parseDescribeCluster(described(PROOF_CONTEXT, PROOF_CLUSTER), SBX)).toMatchObject({ error: expect.stringMatching(/is not/) });
    expect(parseDescribeCluster(described(SANDBOX_CONTEXT, SANDBOX_CLUSTER))).toMatchObject({ error: expect.stringMatching(/is not/) });
  });
  test("the generated kubeconfig gets a token for the sandbox cluster only", () => {
    const d = parseDescribeCluster(described(SANDBOX_CONTEXT, SANDBOX_CLUSTER), SBX);
    if ("error" in d) throw new Error(d.error);
    const kc = JSON.parse(proofKubeconfig(d, SANDBOX_CONTEXT, SBX));
    expect(kc.users[0].user.exec.args).toEqual(["eks", "get-token", "--cluster-name", SANDBOX_CLUSTER, "--region", "us-west-2", "--output", "json"]);
  });
  const fake = (account: string, arn: string) => {
    const calls: string[][] = [];
    const run = (argv: string[]) => {
      calls.push(argv);
      if (argv[1] === "sts") return { stdout: `${argv[4] === "Arn" ? arn : account}\n`, status: 0 };
      if (argv[2] === "describe-cluster") return { stdout: described(SANDBOX_CONTEXT, SANDBOX_CLUSTER), status: 0 };
      return { stdout: "", status: 1 };
    };
    return { calls, run };
  };
  test("the cluster is described only after the caller is exactly the agent-session role", () => {
    const wrong = fake(SANDBOX_ACCOUNT_ID, "arn:aws:sts::361835732085:assumed-role/admin/me");
    const o = observeIdentity(sandboxConfig(), wrong.run, SBX, SESSION);
    expect(o.bindError).toBe("caller is not the pippen agent-session role session for --pippen-session");
    expect(wrong.calls.some((c) => c[2] === "describe-cluster")).toBe(false);
    const other = fake(PROOF_ACCOUNT_ID, SANDBOX_CALLER_ARN);
    expect(observeIdentity(sandboxConfig(), other.run, SBX, SESSION).bindError).toBe("caller is not the proof account");
    expect(other.calls).toHaveLength(1);
    const right = fake(SANDBOX_ACCOUNT_ID, SANDBOX_CALLER_ARN);
    const none = fake(SANDBOX_ACCOUNT_ID, SANDBOX_CALLER_ARN);
    expect(observeIdentity(sandboxConfig(), none.run, SBX).bindError).toMatch(/no valid --pippen-session/);
    expect(none.calls).toHaveLength(1);
    const stale = fake(SANDBOX_ACCOUNT_ID, "arn:aws:sts::361835732085:assumed-role/agent-session/agent-aws-proof");
    expect(observeIdentity(sandboxConfig(), stale.run, SBX, SESSION).bindError).toMatch(/not the pippen agent-session role session/);
    expect(stale.calls.some((c) => c[2] === "describe-cluster")).toBe(false);
    const r = observeIdentity(sandboxConfig(), right.run, SBX, SESSION);
    expect(r.identity).toMatchObject({ accountId: SANDBOX_ACCOUNT_ID, callerArn: SANDBOX_CALLER_ARN, pippenSessionId: SESSION, kubeContextCluster: SANDBOX_CONTEXT });
    expect(right.calls.slice(0, 3)).toEqual([
      ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"],
      ["aws", "sts", "get-caller-identity", "--query", "Arn", "--output", "text"],
      ["aws", "eks", "describe-cluster", "--name", SANDBOX_CLUSTER, "--region", "us-west-2", "--output", "json"],
    ]);
  });
});

describe("pippen sandbox target: config and CLI", () => {
  test("the sandbox preset validates and restates the sandbox constants", () => {
    const c = sandboxConfig();
    expect(validateConfig(c)).toEqual([]);
    expect(c).toMatchObject({ stage: SANDBOX_STAGE, accountId: SANDBOX_ACCOUNT_ID, clusterName: SANDBOX_CLUSTER, kubeContext: SANDBOX_CONTEXT, locker: { bucket: SANDBOX_LOCKER_BUCKET }, ingress: { hostname: SANDBOX_HOST } });
    expect(defaultInputsPath("pippen-sandbox")).toBe(".96bulls/aws-proof-sandbox/inputs.json");
  });
  test("config cannot point the sandbox at sessions-dev values (or anything else)", () => {
    const issues = (p: Record<string, unknown>) => proofConfigIssues({ ...sandboxConfig(), ...p } as never);
    expect(issues({ kubeContext: PROOF_CONTEXT })).toEqual([`kubeContext must be "${SANDBOX_CONTEXT}" (got "${PROOF_CONTEXT}")`]);
    expect(issues({ locker: { bucket: PROOF_LOCKER_BUCKET } })[0]).toMatch(/locker.bucket must be/);
    expect(issues({ ingress: { hostname: PROOF_HOST, burst: {} } })[0]).toMatch(/ingress.hostname must be/);
    expect(issues({ region: "us-east-1" })[0]).toMatch(/region must be/);
    expect(issues({ backupBucket: PROOF_BACKUP_BUCKET })[0]).toMatch(/backupBucket must be/);
  });
  test("an inputs file cannot change the sandbox account, cluster or stage", () => {
    const dir = mkdtempSync(join(tmpdir(), "aws-proof-sbx-"));
    const p = join(dir, "inputs.json");
    writeFileSync(p, JSON.stringify({ accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, stage: PROOF_STAGE }));
    expect(loadConfig(p, "pippen-sandbox")).toMatchObject({ accountId: SANDBOX_ACCOUNT_ID, clusterName: SANDBOX_CLUSTER, stage: SANDBOX_STAGE });
  });
  test("--pippen-session takes only a strict lease uuid, only on the sandbox target", () => {
    expect(parseArgs(["--target", "pippen-sandbox", "--pippen-session", SESSION]).pippenSession).toBe(SESSION);
    expect(() => parseArgs(["--target", "pippen-sandbox", "--pippen-session", "aws-proof"])).toThrow(/lowercase v4 UUID/);
    expect(() => parseArgs(["--target", "pippen-sandbox", "--pippen-session", SESSION.toUpperCase()])).toThrow(/lowercase v4 UUID/);
    expect(() => parseArgs(["--pippen-session", SESSION])).toThrow(/pippen-sandbox only/);
    expect(() => parseArgs(["--target", "pippen-sandbox", "--pippen-session", SESSION, "--pippen-session", SESSION])).toThrow(/once/);
  });
  test("--target pippen-sandbox is explicit; unknown targets are refused", () => {
    expect(parseArgs(["--target", "pippen-sandbox"]).target).toBe("pippen-sandbox");
    expect(parseArgs([]).target).toBe("proof");
    expect(() => parseArgs(["--target", "sandbox"])).toThrow(/unknown target/);
    expect(parseArgs(["--target", "pippen-sandbox", "teardown", "--post-destroy"]).postDestroy).toBe(true);
    expect(() => parseArgs(["--target", "kind-readonly", "teardown", "--post-destroy"])).toThrow(/proof target only/);
  });
  test("a prior report from the sessions-dev target is not admitted for the sandbox", () => {
    const t = TARGET_VALUES["pippen-sandbox"];
    const expectSbx = { stage: SBX.stage, clusterName: SBX.clusterName, kubeContext: t.context, accountId: SBX.accountId, region: SBX.region };
    const devReport = buildReport({ provenance: PROV, runId: "aws-proof-x", generatedAt: "2026-10-31T03:00:01.000Z", mode: "live", target: { stage: PROOF_STAGE, clusterName: PROOF_CLUSTER, kubeContext: PROOF_CONTEXT, accountId: PROOF_ACCOUNT_ID, region: "us-west-2" }, backends: { metadata: "m", transcript: "none" }, checks: [] });
    expect(admitPrior(canonicalReportBytes(devReport), expectSbx, Date.UTC(2026, 10, 1))).toMatchObject({ error: expect.stringMatching(/is not the proof target/) });
  });
});

describe("pippen sandbox target: stage plan needs pippenSandbox", () => {
  const files = stagePlanFiles(SANDBOX_STAGE);
  const planText = "Plan · aws-proof-sandbox\nComputing plan\nPlan ready (4.2s)\nPlan: 1 to create\n[cluster] create\n";
  const baseInputs = {
    accountId: SANDBOX_ACCOUNT_ID, region: "us-west-2",
    pippenSandbox: { permissionsBoundaryArn: "arn:aws:iam::361835732085:policy/pippen-sandbox-boundary" },
    eks: { chartVersion: "0.3.0", imageDigest: `sha256:${"5".repeat(64)}`, relayhistoryOrigin: `https://${SANDBOX_HOST}`, ingress: { hostname: SANDBOX_HOST }, relayauth: { chartVersion: "0.1.0" }, relaycast: { chartVersion: "0.1.0", imageDigest: `sha256:${"6".repeat(64)}` } },
  };
  const reader = (inputs: unknown, o: { stage?: string; account?: string; plan?: string } = {}) => {
    const text = JSON.stringify(inputs);
    const plan = o.plan ?? planText;
    const prov = JSON.stringify({ stage: o.stage ?? SANDBOX_STAGE, generatedAt: "2026-11-01T01:00:00.000Z", target: { accountId: o.account ?? SANDBOX_ACCOUNT_ID, region: "us-west-2" }, source: { commit: "e".repeat(40), clean: true }, inputs: { sanitizedPath: files.inputs, sanitizedSha256: sha256Hex(text) }, outputs: { plan: { path: files.plan, sha256: sha256Hex(plan) } } });
    return (p: string) => (p.endsWith(files.provenance) ? prov : p.endsWith(files.plan) ? plan : p.endsWith(files.inputs) ? text : undefined);
  };
  test("its own capture files, and a complete sandbox capture loads", () => {
    expect(files).toEqual({ provenance: "docs/plans/aws-proof-sandbox.provenance.json", plan: "docs/plans/aws-proof-sandbox.plan.txt", inputs: "docs/plans/aws-proof-sandbox.inputs.sanitized.json" });
    expect(loadStagePlan("/r", reader(baseInputs), undefined, SBX)).toMatchObject({ clusterOp: "create", servicesDigest: `sha256:${"5".repeat(64)}` });
  });
  test("refuses a sandbox plan without pippenSandbox, with a boundary in another account, or with accountShared", () => {
    const { pippenSandbox: _p, ...noSandbox } = baseInputs;
    expect(loadStagePlan("/r", reader(noSandbox), undefined, SBX)).toEqual({ error: "stage plan inputs lack pippenSandbox.permissionsBoundaryArn in 361835732085" });
    expect(loadStagePlan("/r", reader({ ...baseInputs, pippenSandbox: { permissionsBoundaryArn: "arn:aws:iam::500268966557:policy/x" } }), undefined, SBX)).toMatchObject({ error: expect.stringMatching(/lack pippenSandbox/) });
    expect(loadStagePlan("/r", reader({ ...baseInputs, accountShared: { ownerStage: "aws-sessions-dev" } }), undefined, SBX)).toMatchObject({ error: expect.stringMatching(/must not set accountShared/) });
  });
  test("refuses a plan for another stage, account, host or plan header", () => {
    expect(loadStagePlan("/r", reader(baseInputs, { stage: PROOF_STAGE }), undefined, SBX)).toEqual({ error: "stage plan is for aws-proof, not aws-proof-sandbox" });
    expect(loadStagePlan("/r", reader(baseInputs, { account: PROOF_ACCOUNT_ID }), undefined, SBX)).toEqual({ error: "stage plan targets another account or region" });
    expect(loadStagePlan("/r", reader({ ...baseInputs, accountId: PROOF_ACCOUNT_ID }), undefined, SBX)).toEqual({ error: "stage plan inputs target another account or region" });
    expect(loadStagePlan("/r", reader({ ...baseInputs, eks: { ...baseInputs.eks, ingress: { hostname: PROOF_HOST } } }), undefined, SBX)).toEqual({ error: "stage plan inputs name another host or origin" });
    expect(loadStagePlan("/r", reader(baseInputs, { plan: planText.replace("aws-proof-sandbox", "aws-proof") }), undefined, SBX)).toMatchObject({ error: expect.stringMatching(/unrecognized plan line: Plan · aws-proof$/) });
  });
  test("the sessions-dev loader never reads the sandbox capture", () => {
    expect(loadStagePlan("/r", reader(baseInputs))).toMatchObject({ error: expect.stringMatching(/docs\/plans\/aws-proof\.provenance\.json is missing/) });
  });
});

describe("pippen sandbox target: teardown is the pippen sweep, verified after", () => {
  test("teardown prints `pippen sandbox down aws-proof` and lists the sandbox stage tag", () => {
    expect(stageDestroyCommandFor(SANDBOX_STAGE)).toBe(SANDBOX_DESTROY_COMMAND);
    expect(SANDBOX_DESTROY_COMMAND.startsWith("pippen sandbox down aws-proof ")).toBe(true);
    expect(teardownVerifySteps(sandboxConfig()).at(-1)!.argv).toContain("Key=96bulls:stack,Values=aws-proof-sandbox");
  });
  const list = (...arns: Array<string | { arn: string; tags: Record<string, string> }>) =>
    JSON.stringify({ ResourceTagMappingList: arns.map((a) => (typeof a === "string" ? { ResourceARN: a, Tags: [] } : { ResourceARN: a.arn, Tags: Object.entries(a.tags).map(([Key, Value]) => ({ Key, Value })) })) });
  const ROLE = "arn:aws:iam::361835732085:role/pippen/agent-session";
  const queried: string[] = [];
  const fake = (o: { tagged?: string; swept?: string; alchemy?: string; baseline?: string } = {}): Spawn => (argv) => {
    if (argv[2] === "describe-cluster") return { stdout: "", status: 254, stderr: "An error occurred (ResourceNotFoundException) when calling the DescribeCluster operation: No cluster found" };
    if (argv[2] === "list-clusters") return { stdout: "[]", status: 0 };
    if (argv[1] === "resourcegroupstaggingapi") {
      const f = argv[argv.indexOf("--tag-filters") + 1]!;
      queried.push(f);
      const by: Record<string, string | undefined> = { "Key=pippen:sandbox,Values=true": o.swept, "Key=alchemy::stage,Values=aws-proof-sandbox": o.alchemy, "Key=96bulls:stack,Values=aws-proof-sandbox": o.tagged, "Key=pippen:baseline,Values=true": o.baseline };
      return { stdout: by[f] ?? JSON.stringify({ ResourceTagMappingList: [] }), status: 0 };
    }
    if (argv[2] === "head-bucket") return { stdout: "", status: 254, stderr: "An error occurred (404) when calling the HeadBucket operation: Not Found" };
    return { stdout: "", status: 1 };
  };
  const run = async (spawn: Spawn, identity: Omit<GuardInput, "mode" | "mutating"> = { confirmLive: true, accountId: SANDBOX_ACCOUNT_ID, callerArn: SANDBOX_CALLER_ARN, clusterName: SANDBOX_CLUSTER, kubeContext: SANDBOX_CONTEXT, postDestroy: true }) => {
    const r = new Runner({ mode: "live", identity, fence: SANDBOX_FENCE, awsOnly: true, spawn, print: () => {} });
    const ctx = newCtx({ ...sandboxConfig(), sessions: 2 }, r, "aws-proof-post-sbx", {});
    ctx.print = () => {};
    return postDestroyTeardown(ctx);
  };
  test("post-destroy checks the stage tag, the sweeper tag and both sandbox buckets", async () => {
    const res = await run(fake());
    expect(res.status).toBe("pass");
    const cmds = res.steps.map((s) => s.command);
    expect(cmds.some((c) => c.includes("Key=96bulls:stack,Values=aws-proof-sandbox"))).toBe(true);
    expect(cmds.some((c) => c.includes("Key=pippen:sandbox,Values=true"))).toBe(true);
    expect(cmds.some((c) => c.includes("Key=alchemy::stage,Values=aws-proof-sandbox"))).toBe(true);
    expect(cmds.some((c) => c.includes(SANDBOX_LOCKER_BUCKET))).toBe(true);
    expect(cmds.some((c) => c.includes(SANDBOX_BACKUP_BUCKET))).toBe(true);
    expect(cmds.some((c) => c.includes("500268966557"))).toBe(false);
  });
  test("a resource left by the sweep, tagged only pippen:sandbox=true, is FAIL", async () => {
    const left = JSON.stringify({ ResourceTagMappingList: [{ ResourceARN: "arn:aws:ec2:us-west-2:361835732085:volume/vol-1" }] });
    const res = await run(fake({ swept: left }));
    expect(res.status).toBe("fail");
    expect(res.failures).toContain("still present after stage destroy: aws arn:aws:ec2:us-west-2:361835732085:volume/vol-1");
  });
  test("the fresh agent-session role (exact ARN) is not a leftover", async () => {
    const res = await run(fake({ swept: list({ arn: ROLE, tags: { "pippen:sandbox": "true", "pippen:agent": "aws-proof", "pippen:session": "s-2" } }) }));
    expect(res.failures).toEqual([]);
    expect(res.status).toBe("pass");
  });
  test("any second pippen:sandbox resource beside the role still fails, as do look-alike role ARNs", async () => {
    const vol = "arn:aws:ec2:us-west-2:361835732085:volume/vol-2";
    const res = await run(fake({ swept: list({ arn: ROLE, tags: { "pippen:sandbox": "true" } }, { arn: vol, tags: { "pippen:sandbox": "true" } }) }));
    expect(res.status).toBe("fail");
    expect(res.failures).toEqual([`still present after stage destroy: aws ${vol}`]);
    for (const lookalike of ["arn:aws:iam::361835732085:role/agent-session", "arn:aws:iam::361835732085:role/pippen/agent-session-2", "arn:aws:iam::500268966557:role/pippen/agent-session"]) {
      const r2 = await run(fake({ swept: list(lookalike) }));
      expect(r2.failures).toEqual([`still present after stage destroy: aws ${lookalike}`]);
    }
    // Exempt only by ARN, never by tag: a pippen:baseline tag on a stage-tagged leftover does not excuse it.
    const kms = "arn:aws:s3:::96bulls-aws-proof-sandbox-trail-361835732085";
    expect((await run(fake({ tagged: list({ arn: kms, tags: { "pippen:baseline": "true" } }) }))).status).toBe("fail");
  });
  test("an alchemy::stage-tagged leftover fails; the 96bulls:stack listing never exempts more than the role", async () => {
    const sg = "arn:aws:ec2:us-west-2:361835732085:security-group/sg-1";
    expect((await run(fake({ alchemy: list(sg) }))).failures).toEqual([`still present after stage destroy: aws ${sg}`]);
    expect((await run(fake({ tagged: list(sg) }))).failures).toEqual([`still present after stage destroy: aws ${sg}`]);
  });
  test("Pippen's baseline (pippen:baseline=true) is never queried, so it cannot trip the check", async () => {
    queried.length = 0;
    const res = await run(fake({ baseline: list({ arn: "arn:aws:iam::361835732085:policy/pippen-agent-session-boundary", tags: { "pippen:baseline": "true" } }, { arn: "arn:aws:iam::361835732085:role/pippen/pippen-sweeper", tags: { "pippen:baseline": "true" } }) }));
    expect(res.status).toBe("pass");
    expect(queried.sort()).toEqual(["Key=96bulls:stack,Values=aws-proof-sandbox", "Key=alchemy::stage,Values=aws-proof-sandbox", "Key=pippen:sandbox,Values=true"]);
  });
  test("post-destroy runs with a fresh session of the recreated role", async () => {
    const res = await run(fake(), { confirmLive: true, accountId: SANDBOX_ACCOUNT_ID, callerArn: "arn:aws:sts::361835732085:assumed-role/agent-session/pippen-agent-9f1e", clusterName: SANDBOX_CLUSTER, kubeContext: SANDBOX_CONTEXT, postDestroy: true });
    expect(res.status).toBe("pass");
  });
  test("withoutExempt leaves the sessions-dev listing byte-for-byte unchanged", () => {
    const raw = list("arn:aws:iam::500268966557:role/pippen/agent-session") + " ";
    expect(withoutExempt(PROOF_STAGE, raw)).toBe(raw);
  });
  test("one ARN tagged both ways is reported once", async () => {
    const left = JSON.stringify({ ResourceTagMappingList: [{ ResourceARN: "arn:aws:s3:::x" }] });
    const res = await run(fake({ tagged: left, swept: left }));
    expect(res.failures.filter((f) => f.includes("arn:aws:s3:::x"))).toHaveLength(1);
  });
  test("post-destroy with the wrong caller runs nothing", async () => {
    const calls: string[][] = [];
    const res = await run((argv) => (calls.push(argv), { stdout: "", status: 0 }), { confirmLive: true, accountId: SANDBOX_ACCOUNT_ID, callerArn: "arn:aws:sts::361835732085:assumed-role/admin/me", clusterName: SANDBOX_CLUSTER, kubeContext: SANDBOX_CONTEXT, postDestroy: true });
    expect(calls).toHaveLength(0);
    expect(res.steps.every((s) => s.status === "refused" && s.code === "wrong_caller")).toBe(true);
  });
  test("the sessions-dev post-destroy runs no sweeper-tag query", async () => {
    const calls: string[][] = [];
    const spawn = fake();
    const r = new Runner({ mode: "live", identity: { confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: PROOF_CONTEXT, postDestroy: true }, fence: PROOF_FENCE, awsOnly: true, spawn: (...a) => (calls.push(a[0]), spawn(...a)), print: () => {} });
    const ctx = newCtx({ ...defaultConfig(), sessions: 2 }, r, "aws-proof-post-dev", {});
    ctx.print = () => {};
    expect((await postDestroyTeardown(ctx)).status).toBe("pass");
    expect(calls.map((c) => c.join(" "))).toEqual([
      "aws eks describe-cluster --name 96bulls-aws-proof --region us-west-2 --output json",
      "aws eks list-clusters --region us-west-2 --output json",
      "aws resourcegroupstaggingapi get-resources --tag-filters Key=96bulls:stack,Values=aws-proof --region us-west-2 --output json",
      "aws s3api head-bucket --bucket 96bulls-aws-proof-locker-500268966557 --region us-west-2 --output json",
      "aws s3api head-bucket --bucket 96bulls-aws-proof-hoopdb-backups-500268966557 --region us-west-2 --output json",
    ]);
  });
});
