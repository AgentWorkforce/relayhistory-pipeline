import { describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "../cli.ts";
import { loadConfig } from "../config.ts";
import { Runner, shellQuote, type Spawn } from "../exec.ts";
import { defaultConfig } from "../config.ts";
import { fenceFor } from "../fence.ts";
import { assertMayRun, assertProofTarget, GuardError, LOCAL_READONLY_CONTEXT, PROOF_ACCOUNT_ID, PROOF_CLUSTER, type GuardInput } from "../guard.ts";

const ARN = `arn:aws:eks:us-west-2:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`;
const fence = fenceFor("proof");
const ok: GuardInput = { mode: "live", confirmLive: true, mutating: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, kubeContextCluster: ARN, clusterBound: true };

const code = (f: () => void) => {
  try {
    f();
    return "allowed";
  } catch (e) {
    return e instanceof GuardError ? e.code : "other";
  }
};

describe("guard: the proof account and cluster only", () => {
  test("the proof account + cluster + --confirm-live is allowed", () => {
    expect(code(() => assertMayRun(ok))).toBe("allowed");
  });
  test("dry-run never needs a target", () => {
    expect(code(() => assertMayRun({ ...ok, mode: "dry-run", confirmLive: false, accountId: undefined, clusterName: "anything" }))).toBe("allowed");
  });
  test("refuses without --confirm-live, read-only included", () => {
    expect(code(() => assertMayRun({ ...ok, confirmLive: false }))).toBe("missing_confirm_live");
    expect(code(() => assertMayRun({ ...ok, mode: "read-only", mutating: false, confirmLive: false }))).toBe("missing_confirm_live");
  });
  test("refuses any other account", () => {
    expect(code(() => assertMayRun({ ...ok, accountId: "123456789012" }))).toBe("wrong_account");
  });
  test("refuses when the caller identity was not verified", () => {
    expect(code(() => assertMayRun({ ...ok, accountId: undefined }))).toBe("unverified_identity");
  });
  test("refuses any other cluster name", () => {
    expect(code(() => assertMayRun({ ...ok, clusterName: "96bulls-aws-proof2" }))).toBe("wrong_cluster");
    expect(code(() => assertMayRun({ ...ok, clusterName: "96bulls-aws-sandbox-x" }))).toBe("wrong_cluster");
  });
  test("refuses a kube context that points at another account or cluster", () => {
    expect(code(() => assertMayRun({ ...ok, kubeContextCluster: `arn:aws:eks:us-west-2:111111111111:cluster/${PROOF_CLUSTER}` }))).toBe("context_mismatch");
    expect(code(() => assertMayRun({ ...ok, kubeContextCluster: `arn:aws:eks:us-west-2:${PROOF_ACCOUNT_ID}:cluster/other` }))).toBe("context_mismatch");
    expect(code(() => assertMayRun({ ...ok, kubeContextCluster: undefined }))).toBe("context_mismatch");
  });
  test("refuses production-shaped names even if everything else matches", () => {
    expect(code(() => assertMayRun({ ...ok, kubeContext: "sessions-dev" }))).toBe("production_name");
    expect(code(() => assertMayRun({ ...ok, kubeContextCluster: "arn:aws:eks:us-west-2:500268966557:cluster/96bulls-prod" }))).toBe("production_name");
  });
  test("the local kind cluster is read-only: reads allowed, mutations and live refused", () => {
    const kind = { ...ok, kubeContext: LOCAL_READONLY_CONTEXT, clusterName: LOCAL_READONLY_CONTEXT, accountId: undefined, kubeContextCluster: undefined, confirmLive: false };
    expect(code(() => assertMayRun({ ...kind, mode: "read-only", mutating: false }))).toBe("allowed");
    expect(code(() => assertMayRun({ ...kind, mode: "read-only", mutating: true }))).toBe("local_is_read_only");
    expect(code(() => assertMayRun({ ...kind, mode: "live", mutating: false }))).toBe("local_is_read_only");
  });
  test("assertProofTarget is the same check the CLI runs before any step", () => {
    expect(code(() => assertProofTarget({ ...ok, confirmLive: false }))).toBe("missing_confirm_live");
  });
});

describe("runner: refusals never spawn", () => {
  const spy = () => {
    const calls: string[][] = [];
    const spawn: Spawn = (argv) => (calls.push(argv), { stdout: "out", status: 0 });
    return { calls, spawn };
  };
  const step = { id: "x", argv: ["kubectl", "--context", ARN, "-n", "96bulls", "delete", "pod", "hoopdb-1"], mutating: true, touches: ["k8s"], approval: "khaliq" as const };

  test("dry-run plans and does not spawn", () => {
    const s = spy();
    const r = new Runner({ kubeconfigPath: "/test/kubeconfig.json", mode: "dry-run", identity: ok, spawn: s.spawn, print: () => {}, fence });
    expect(r.run(step).record.status).toBe("planned");
    expect(s.calls).toHaveLength(0);
  });
  test("read-only skips mutating steps", () => {
    const s = spy();
    const r = new Runner({ kubeconfigPath: "/test/kubeconfig.json", mode: "read-only", identity: ok, spawn: s.spawn, print: () => {}, fence });
    expect(r.run(step).record.status).toBe("skipped");
    expect(r.run({ id: "r", argv: ["kubectl", "--context", ARN, "-n", "96bulls", "get", "pods", "-o", "json"], mutating: false, touches: [], approval: "operator" }).record.status).toBe("ran");
    expect(s.calls).toHaveLength(1);
  });
  test("live without --confirm-live is refused with a code and does not spawn", () => {
    const s = spy();
    const r = new Runner({ kubeconfigPath: "/test/kubeconfig.json", mode: "live", identity: { ...ok, confirmLive: false }, spawn: s.spawn, print: () => {}, fence });
    const out = r.run(step);
    expect(out.record.status).toBe("refused");
    expect(out.record.code).toBe("missing_confirm_live");
    expect(s.calls).toHaveLength(0);
  });
  test("live against a non-proof account is refused and does not spawn", () => {
    const s = spy();
    const r = new Runner({ kubeconfigPath: "/test/kubeconfig.json", mode: "live", identity: { ...ok, accountId: "999999999999" }, spawn: s.spawn, print: () => {}, fence });
    expect(r.run(step).record.code).toBe("wrong_account");
    expect(s.calls).toHaveLength(0);
  });
  test("a failing command is recorded as failed with its exit code, not its output", () => {
    const r = new Runner({ kubeconfigPath: "/test/kubeconfig.json", mode: "live", identity: ok, spawn: () => ({ stdout: "secret-ish", status: 3 }), print: () => {}, fence });
    const out = r.run(step);
    expect(out.record).toMatchObject({ status: "failed", exitCode: 3, code: "exit_3" });
    expect(JSON.stringify(out.record)).not.toContain("secret-ish");
  });
  test("printed commands are exactly quoted", () => {
    expect(shellQuote(["a", "b c", "it's", "jsonpath={.x}"])).toBe(`a 'b c' 'it'\\''s' 'jsonpath={.x}'`);
  });
});

describe("cli flags", () => {
  test("default is a dry run of every check", () => {
    const a = parseArgs([]);
    expect(a.mode).toBe("dry-run");
    expect(a.checks.length).toBeGreaterThanOrEqual(10);
  });
  test("--confirm-live alone is live; with --read-only it is read-only", () => {
    expect(parseArgs(["--confirm-live"]).mode).toBe("live");
    expect(parseArgs(["--confirm-live", "--read-only"]).mode).toBe("read-only");
  });
  test("kind never takes a live run", () => {
    expect(() => parseArgs(["--target", "kind-readonly", "--confirm-live"])).toThrow();
  });
  test("unknown checks and bad session counts are refused", () => {
    expect(() => parseArgs(["nuke"])).toThrow();
    expect(() => parseArgs(["--sessions", "1"])).toThrow();
  });
});

describe("inputs file cannot widen the target or carry secrets", () => {
  const dir = mkdtempSync(join(tmpdir(), "aws-proof-inputs-"));
  test("accountId, clusterName and stage in inputs.json are ignored", () => {
    const p = join(dir, "inputs.json");
    writeFileSync(p, JSON.stringify({ accountId: "111111111111", clusterName: "96bulls-prod", stage: "prod", sessions: 7 }));
    const c = loadConfig(p, "proof");
    expect(c.accountId).toBe(PROOF_ACCOUNT_ID);
    expect(c.clusterName).toBe(PROOF_CLUSTER);
    expect(c.sessions).toBe(7);
  });
  test("secret-like keys anywhere are refused", () => {
    const p = join(dir, "bad.json");
    writeFileSync(p, JSON.stringify({ relayauth: { namespace: "x", bootstrapKey: "rak_x" } }));
    expect(() => loadConfig(p, "proof")).toThrow(/must not carry secrets/);
  });
});
