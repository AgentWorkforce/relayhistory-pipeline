/**
 * #74 review round 3 (at 6d1b690): each counterexample reproduced exactly.
 */
import { describe, expect, test } from "bun:test";
import { PROV } from "./prov.ts";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { main } from "../cli.ts";
import { Runner, type Spawn } from "../exec.ts";
import { PROOF_CONTEXT, PROOF_FENCE } from "../fence.ts";
import { assertProofTarget, GuardError, PROOF_ACCOUNT_ID, PROOF_CLUSTER } from "../guard.ts";
import { admitStep, stepFenceIssue } from "../ops.ts";
import { admitPrior, buildReport, canonicalReportBytes, type CheckResult } from "../report.ts";

const ARN = PROOF_CONTEXT;
const EU = `arn:aws:eks:eu-west-1:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`;
const k = (...a: string[]) => ["kubectl", "--context", ARN, ...a];
const identity = { confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, kubeContextCluster: ARN, clusterBound: true };
const code = (f: () => void) => {
  try {
    f();
    return "allowed";
  } catch (e) {
    return e instanceof GuardError ? e.code : "other";
  }
};

describe("(1) the EKS ARN region is compared", () => {
  test("the west context name mapped to an eu-west-1 cluster ARN is refused", () => {
    expect(code(() => assertProofTarget({ ...identity, kubeContextCluster: EU }))).toBe("wrong_region");
  });
  test("an eu-west-1 context name is refused even if its cluster entry looks right", () => {
    expect(code(() => assertProofTarget({ ...identity, kubeContext: EU }))).toBe("wrong_region");
  });
  test("the exact proof ARN in us-west-2 is allowed", () => {
    expect(code(() => assertProofTarget(identity))).toBe("allowed");
  });
});

describe("(2) a missing kube context refuses", () => {
  test("kubectl and helm without a context are refused, and never spawn", () => {
    expect(stepFenceIssue(["kubectl", "-n", "96bulls", "delete", "pod", "hoopdb-1"], PROOF_FENCE)).toBe("context_missing");
    expect(stepFenceIssue(["kubectl", "get", "pods"], PROOF_FENCE)).toBe("context_missing");
    expect(stepFenceIssue(["helm", "-n", "96bulls", "uninstall", "96bulls", "--wait"], PROOF_FENCE)).toBe("context_missing");
    let spawned = 0;
    const r = new Runner({ kubeconfigPath: "/test/kubeconfig.json", mode: "live", identity, fence: PROOF_FENCE, print: () => {}, spawn: () => (spawned++, { stdout: "", status: 0 }) });
    expect(r.run({ id: "x", argv: ["kubectl", "-n", "96bulls", "delete", "pod", "hoopdb-1"], mutating: true, touches: [], approval: "khaliq" }).record.code).toBe("context_missing");
    expect(spawned).toBe(0);
  });
  test("a second --context after the fenced one is refused", () => {
    expect(stepFenceIssue(k("--context", "other", "-n", "96bulls", "get", "pods"), PROOF_FENCE)).toBe("flag_repeated");
  });
});

describe("(3) no shell or env wrapper can carry a command", () => {
  test("sh -c / bash -c / env / absolute paths are refused outright", () => {
    const inner = `kubectl --context ${ARN} -n kube-system delete pod coredns`;
    for (const argv of [["sh", "-c", inner], ["bash", "-c", inner], ["/bin/sh", "-c", inner], ["env", ...inner.split(" ")], ["/usr/local/bin/kubectl", "--context", ARN, "get", "pods"], ["xargs", "kubectl"]])
      expect(stepFenceIssue(argv, PROOF_FENCE)).toBe("program_not_allowlisted");
  });
  test("kube-system (and any non-allowlisted namespace) is refused directly too", () => {
    expect(stepFenceIssue(k("-n", "kube-system", "delete", "pod", "coredns"), PROOF_FENCE)).toBe("namespace_outside_fence");
    expect(stepFenceIssue(k("-n", "default", "get", "pods"), PROOF_FENCE)).toBe("namespace_outside_fence");
  });
});

describe("(4) mutations need an exact (verb, kind, namespace, name) entry", () => {
  test("arbitrary service/deploy/db/CNPG/Helm names are refused", () => {
    expect(stepFenceIssue(k("-n", "96bulls", "delete", "service/attacker"), PROOF_FENCE)).toBe("delete_not_allowlisted");
    expect(stepFenceIssue(k("-n", "96bulls", "delete", "deploy/attacker"), PROOF_FENCE)).toBe("delete_not_allowlisted");
    expect(stepFenceIssue(k("-n", "96bulls", "rollout", "restart", "deploy/attacker"), PROOF_FENCE)).toBe("kubectl_operation_not_allowlisted");
    expect(stepFenceIssue(k("-n", "96bulls", "exec", "-i", "hoopdb-1", "-c", "postgres", "--", "psql", "--dbname=postgres", "-f", "-"), PROOF_FENCE)).toBe("exec_command_not_allowlisted");
    expect(stepFenceIssue(k("-n", "96bulls", "get", "cluster.postgresql.cnpg.io", "attacker"), PROOF_FENCE)).toBe("kubectl_operation_not_allowlisted");
    expect(stepFenceIssue(k("-n", "96bulls", "delete", "cluster.postgresql.cnpg.io", "attacker"), PROOF_FENCE)).toBe("delete_not_allowlisted");
    expect(stepFenceIssue(["helm", "--kube-context", ARN, "-n", "96bulls", "delete", "attacker"], PROOF_FENCE)).toBe("release_outside_fence");
    expect(stepFenceIssue(["helm", "--kube-context", ARN, "-n", "96bulls", "delete", "96bulls"], PROOF_FENCE)).toBe("helm_operation_not_allowlisted");
    expect(stepFenceIssue(["helm", "--kube-context", ARN, "-n", "relayauth", "uninstall", "relayauth", "--wait"], PROOF_FENCE)).toBe("helm_operation_not_allowlisted");
    expect(stepFenceIssue(k("-n", "96bulls", "apply", "-f", "-"), PROOF_FENCE, { stdin: JSON.stringify({ apiVersion: "v1", kind: "Secret", metadata: { name: "x", namespace: "96bulls" } }) })).toBe("apply_manifest_outside_fence");
  });
  test("relayauth is read-only except the named rollout restart", () => {
    expect(admitStep({ argv: k("-n", "relayauth", "rollout", "restart", "deployment/relayauth") }, PROOF_FENCE)).toMatchObject({ ok: true, mutating: true });
    expect(admitStep({ argv: k("-n", "relayauth", "logs", "deployment/relayauth", "--all-containers") }, PROOF_FENCE)).toMatchObject({ ok: true, mutating: false });
    for (const argv of [k("-n", "relayauth", "delete", "pod", "relayauth-0"), k("-n", "relayauth", "run", "x", "--image=a/b@sha256:" + "a".repeat(64)), k("-n", "relayauth", "apply", "-f", "-")])
      expect(stepFenceIssue(argv, PROOF_FENCE, { stdin: "{}" })).toBe("relayauth_read_only");
  });
  test("the runner decides mutation from the parsed operation: a mutation labelled as a read is refused and never spawns", () => {
    let spawned = 0;
    const spawn: Spawn = () => (spawned++, { stdout: "", status: 0 });
    const live = new Runner({ kubeconfigPath: "/test/kubeconfig.json", mode: "live", identity, fence: PROOF_FENCE, print: () => {}, spawn });
    const out = live.run({ id: "x", argv: k("-n", "relayauth", "rollout", "restart", "deployment/relayauth"), mutating: false, touches: [], approval: "operator" });
    expect(out.record).toMatchObject({ status: "refused", code: "mutation_mislabeled" });
    expect(spawned).toBe(0);
  });
  test("read-only mode skips anything the parser classifies as mutating", () => {
    let spawned = 0;
    const ro = new Runner({ kubeconfigPath: "/test/kubeconfig.json", mode: "read-only", identity, fence: PROOF_FENCE, print: () => {}, spawn: () => (spawned++, { stdout: "", status: 0 }) });
    expect(ro.run({ id: "x", argv: k("-n", "96bulls", "rollout", "restart", "deployment/96bulls-worker-referee"), mutating: true, touches: [], approval: "khaliq" }).record.status).toBe("skipped");
    expect(spawned).toBe(0);
  });
  test("in-process actions are never spawned", () => {
    let spawned = 0;
    const r = new Runner({ kubeconfigPath: "/test/kubeconfig.json", mode: "live", identity, fence: PROOF_FENCE, print: () => {}, spawn: () => (spawned++, { stdout: "", status: 0 }) });
    expect(r.run({ id: "x", argv: ["HTTP", "GET", "https://proof-sessions.agentrelay.com/healthz"], mutating: false, touches: [], approval: "operator" }).record.code).toBe("in_process_not_spawnable");
    expect(spawned).toBe(0);
  });
});

describe("(5) the prior report is admitted completely before any step", () => {
  const T = (m: number) => new Date(Date.UTC(2026, 9, 30, 12, m)).toISOString();
  const EXPECT = { stage: "aws-proof", clusterName: PROOF_CLUSTER, kubeContext: ARN, accountId: PROOF_ACCOUNT_ID, region: "us-west-2" };
  const step = { id: "s", command: "kubectl get pods", mutating: false, touches: [], approval: "operator" as const, status: "ran" as const };
  const res = (at: string, fin = at): CheckResult => ({ check: "fresh-install", status: "pass", startedAt: at, finishedAt: fin, summary: "ok", metrics: {}, failures: [], evidence: [], steps: [step] });
  const prior = (target = EXPECT, checks = [res(T(0))], generatedAt = T(1)) =>
    buildReport({ provenance: PROV, runId: "aws-proof-run-1", generatedAt, mode: "live", target, backends: { metadata: "m", transcript: "none" }, checks });
  const NOW = Date.UTC(2026, 9, 30, 13);
  test("a canonical live report for stage evil / cluster attacker / context attacker is refused (not just 'equal to current')", () => {
    const evil = { stage: "evil", clusterName: "attacker", kubeContext: "attacker", accountId: PROOF_ACCOUNT_ID, region: "us-west-2" };
    expect(admitPrior(canonicalReportBytes(prior(evil)), EXPECT, NOW)).toEqual({ error: "prior report stage is not the proof target" });
  });
  test("a check that finishes after generatedAt is refused", () => {
    expect(admitPrior(canonicalReportBytes(prior(EXPECT, [res(T(0), T(5))], T(1))), EXPECT, NOW)).toMatchObject({ error: expect.stringMatching(/ends after the report was generated/) });
  });
  test("calendar-invalid and non-canonical timestamps are refused", () => {
    const bad = prior(EXPECT, [res("2026-02-30T00:00:00.000Z")], T(1));
    expect(admitPrior(canonicalReportBytes(bad), EXPECT, NOW)).toMatchObject({ error: expect.stringMatching(/timestamps must be ISO-8601/) });
    const noMs = prior(EXPECT, [res("2026-10-30T12:00:00Z")], T(1));
    expect(admitPrior(canonicalReportBytes(noMs), EXPECT, NOW)).toMatchObject({ error: expect.stringMatching(/timestamps must be ISO-8601/) });
  });
  test("a future-dated prior is refused", () => {
    expect(admitPrior(canonicalReportBytes(prior()), EXPECT, Date.UTC(2026, 9, 30, 11))).toEqual({ error: "prior report is dated in the future" });
  });
  test("the admitted prior carries the digest of its exact bytes", () => {
    const a = admitPrior(canonicalReportBytes(prior()), EXPECT, NOW);
    expect("sha256" in a && /^[0-9a-f]{64}$/.test(a.sha256)).toBe(true);
  });
  test("the CLI refuses a wrong-target prior before any AWS call or step (exit 2)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "aws-proof-prior-"));
    const p = join(dir, "report.json");
    writeFileSync(p, canonicalReportBytes(prior({ ...EXPECT, stage: "evil" })));
    const errs: string[] = [];
    const orig = console.error;
    console.error = (m: string) => void errs.push(m);
    try {
      expect(await main(["teardown", "--confirm-live", "--prior-report", p, "--inputs", join(dir, "none.json")], {})).toBe(2);
    } finally {
      console.error = orig;
    }
    expect(errs.join("\n")).toMatch(/refused before any step ran: prior report stage is not the proof target/);
  });
});
