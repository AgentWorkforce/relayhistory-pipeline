import { describe, expect, test } from "bun:test";
import { parseArgs } from "../cli.ts";
import { defaultConfig, kindReadOnlyConfig, validateConfig, type ProofConfig } from "../config.ts";
import { Runner } from "../exec.ts";
import { fenceFor, PROOF_HOST, PROOF_LOCKER_BUCKET, stepFenceIssue } from "../fence.ts";
import { PROOF_ACCOUNT_ID, PROOF_CLUSTER } from "../guard.ts";
import { buildReport, mergeReports, sanitize, validateReport, type CheckResult } from "../report.ts";

const ARN = `arn:aws:eks:us-west-2:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`;
const identity = { confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, kubeContextCluster: ARN };
const cfg = (over: Partial<ProofConfig> = {}): ProofConfig => ({ ...defaultConfig(), ...over });
const issues = (over: Partial<ProofConfig>) => validateConfig(cfg(over));

describe("fix 1: every target is fenced to the proof stage", () => {
  test("the defaults are the proof stage's own and validate", () => {
    const c = defaultConfig();
    expect(c.ingress.hostname).toBe(PROOF_HOST);
    expect(c.historyOrigin).toBe(`https://${PROOF_HOST}`);
    expect(c.locker.bucket).toBe(PROOF_LOCKER_BUCKET);
    expect(validateConfig(c)).toEqual([]);
    expect(JSON.stringify(c)).not.toContain("sessions-dev");
  });
  test("sessions-dev and other non-proof hosts are refused", () => {
    expect(issues({ ingress: { hostname: "sessions-dev.agentrelay.com", burst: {} } }).join("\n")).toMatch(/production- or shared-shaped[\s\S]*ingress.hostname must be proof-sessions/);
    expect(issues({ ingress: { hostname: "sessions.agentrelay.com", burst: {} } })).toContain(`ingress.hostname must be ${PROOF_HOST} (got sessions.agentrelay.com)`);
    expect(issues({ historyOrigin: "https://sessions.agentrelay.com" }).length).toBe(1);
  });
  test("sessions-prod / prod / production names are refused even in the proof account", () => {
    expect(issues({ namespace: "sessions-prod" }).some((i) => i.startsWith("namespace is production"))).toBe(true);
    expect(issues({ release: "sessions-prod" }).some((i) => i.startsWith("release is production"))).toBe(true);
    expect(issues({ coreDeployments: ["96bulls-prod-ingest"] }).some((i) => i.includes("production"))).toBe(true);
    expect(issues({ backupBucket: `96bulls-aws-proof-production-${PROOF_ACCOUNT_ID}` }).some((i) => i.includes("production"))).toBe(true);
  });
  test("buckets, namespaces, releases, deployments, repo and context outside the stage are refused", () => {
    expect(issues({ locker: { bucket: "some-other-bucket", prefix: "" } })).toContain(`locker.bucket must be ${PROOF_LOCKER_BUCKET} (got some-other-bucket)`);
    expect(issues({ backupBucket: "shared-backups" })).toContain(`backupBucket must be 96bulls-aws-proof-<name>-${PROOF_ACCOUNT_ID}`);
    expect(issues({ namespace: "default" })[0]).toMatch(/namespace must be one of/);
    expect(issues({ release: "other" })).toContain("release must be 96bulls (got other)");
    expect(issues({ coreDeployments: ["other-ingest"] })[0]).toMatch(/must belong to release 96bulls/);
    expect(issues({ chartRepo: "https://evil.example/charts" })[0]).toMatch(/chartRepo must be/);
    expect(issues({ kubeContext: "arn:aws:eks:us-west-2:111111111111:cluster/96bulls-aws-proof" })[0]).toMatch(/kubeContext must be/);
  });
  const fence = fenceFor(defaultConfig());
  const k = (...a: string[]) => ["kubectl", "--context", ARN, ...a];
  test("the step fence refuses any argv that names a target outside the fence", () => {
    expect(stepFenceIssue(k("-n", "96bulls", "get", "pods"), fence)).toBeUndefined();
    expect(stepFenceIssue(["aws", "s3api", "list-objects-v2", "--region", "us-west-2", "--bucket", PROOF_LOCKER_BUCKET], fence)).toBeUndefined();
    expect(stepFenceIssue(k("-n", "kube-system", "get", "pods"), fence)).toBe("namespace_outside_fence");
    expect(stepFenceIssue(["kubectl", "--context", "sessions-dev", "get", "pods"], fence)).toBe("production_shaped_target");
    expect(stepFenceIssue(["kubectl", "--context", "other", "get", "pods"], fence)).toBe("context_outside_fence");
    expect(stepFenceIssue(["aws", "s3api", "delete-object", "--region", "us-west-2", "--bucket", "victim", "--key", "k"], fence)).toBe("bucket_outside_fence");
    expect(stepFenceIssue(["sh", "-c", "aws s3 cp s3://victim/x -"], fence)).toBe("bucket_outside_fence");
    expect(stepFenceIssue(["aws", "eks", "list-clusters", "--region", "eu-west-1"], fence)).toBe("region_outside_fence");
    expect(stepFenceIssue(k("-n", "96bulls", "rollout", "restart", "deployment/other"), fence)).toBe("deployment_outside_fence");
    expect(stepFenceIssue(["PROBE", "GET https://example.com/healthz"], fence)).toBe("host_outside_fence");
    expect(stepFenceIssue(["PROBE", `GET https://${PROOF_HOST}/healthz`], fence)).toBeUndefined();
    expect(stepFenceIssue(k("-n", "relaycast", "run", "x", "--env=RC_URL=http://relaycast.relaycast.svc:8787"), fence)).toBeUndefined();
    expect(stepFenceIssue(k("-n", "relaycast", "run", "x", "--env=RC_URL=http://db.kube-system.svc:8787"), fence)).toBe("host_outside_fence");
    expect(stepFenceIssue(["aws", "wafv2", "get-web-acl-for-resource", "--region", "us-west-2", "--resource-arn", "arn:aws:elasticloadbalancing:us-west-2:111111111111:loadbalancer/app/x/y"], fence)).toBe("arn_outside_fence");
  });
  test("the runner refuses an out-of-fence step in every mode, dry-run included, and never spawns", () => {
    for (const mode of ["dry-run", "read-only", "live"] as const) {
      let spawned = 0;
      const r = new Runner({ mode, identity, fence, print: () => {}, spawn: () => (spawned++, { stdout: "", status: 0 }) });
      const out = r.run({ id: "x", argv: ["aws", "s3api", "delete-object", "--region", "us-west-2", "--bucket", "victim", "--key", "k"], mutating: true, touches: [], approval: "khaliq" });
      expect(out.record).toMatchObject({ status: "refused", code: "bucket_outside_fence" });
      expect(spawned).toBe(0);
    }
  });
  test("the kind fence allows only kind's namespaces and no ARNs", () => {
    const kf = fenceFor(kindReadOnlyConfig());
    expect(stepFenceIssue(["kubectl", "--context", "kind-96bulls-e2e", "-n", "96bulls-e2e", "get", "pods"], kf)).toBeUndefined();
    expect(stepFenceIssue(["kubectl", "--context", "kind-96bulls-e2e", "-n", "96bulls", "get", "pods"], kf)).toBe("namespace_outside_fence");
  });
});

describe("fix 2: URL credentials are rejected and never printed", () => {
  test("userinfo anywhere in the config is refused at validation", () => {
    const t = { ...defaultConfig().transcriptSearch, backend: "clickhouse" as const, endpoint: "https://reader:hunter2@abc.clickhouse.cloud:8443", database: "proof", table: "t" };
    expect(validateConfig(cfg({ transcriptSearch: t })).some((i) => i.startsWith("transcriptSearch.endpoint carries URL credentials"))).toBe(true);
    expect(validateConfig(cfg({ search: { mode: "postgrest", url: "https://u:p@x.supabase.co" } })).some((i) => i.includes("URL credentials"))).toBe(true);
  });
  test("a step carrying userinfo is refused and its recorded/printed command is sanitized, in dry-run too", () => {
    const printed: string[] = [];
    const r = new Runner({ mode: "dry-run", identity, fence: fenceFor(defaultConfig()), print: (l) => printed.push(l) });
    const out = r.run({ id: "x", argv: ["curl", "https://reader:hunter2@abc.clickhouse.cloud/"], mutating: false, touches: ["https://reader:hunter2@abc"], approval: "operator" });
    expect(out.record.code).toBe("url_credentials");
    expect(JSON.stringify(out.record)).not.toContain("hunter2");
    expect(printed.join("\n")).not.toContain("hunter2");
  });
  test("the sanitizer strips userinfo of any scheme", () => {
    for (const u of ["https://reader:hunter2@h/x", "postgres://hoop:hunter2@h:5432/db", "rediss://default:hunter2@h:6380", "https://token-only-hunter2@h"])
      expect(sanitize(u)).not.toContain("hunter2");
  });
});

describe("fix 3: the two teardown phases merge into one reproducible report", () => {
  const T = (m: number) => new Date(Date.UTC(2026, 9, 30, 12, m)).toISOString();
  const CH = "ClickHouse Cloud abc.clickhouse.cloud (proof.t; feed: harness-synthetic)";
  const target = { stage: "aws-proof", clusterName: PROOF_CLUSTER, kubeContext: ARN, accountId: PROOF_ACCOUNT_ID, region: "us-west-2" };
  const res = (check: CheckResult["check"], status: CheckResult["status"], at: string): CheckResult => ({ check, status, startedAt: at, finishedAt: at, summary: status, metrics: {}, failures: status === "fail" ? ["x"] : [], evidence: [], steps: [] });
  const ids = ["fresh-install", "planted-trace", "public-ingress", "tenant-isolation", "v1-search-scope", "transcript-search", "stale-summary", "restart-persistence", "upgrade-rollback", "backup-restore", "teardown"] as const;
  const phase1 = buildReport({ runId: "aws-proof-run-1", generatedAt: T(0), mode: "live", target, backends: { metadata: "HoopDb Postgres", transcript: CH }, checks: ids.map((id) => res(id, id === "v1-search-scope" ? "unsupported" : id === "teardown" ? "blocked" : "pass", T(0))) });
  const phase2 = buildReport({ runId: "aws-proof-run-2", generatedAt: T(30), mode: "live", target, backends: { metadata: "HoopDb Postgres", transcript: "none" }, checks: [res("teardown", "pass", T(30))] });
  test("phase 1 alone is incomplete; phase 2 alone is incomplete; merged is PASS", () => {
    expect(phase1.verdict).toBe("incomplete");
    expect(phase2.verdict).toBe("incomplete");
    const m = mergeReports(phase1, "a".repeat(64), phase2);
    if ("error" in m) throw new Error(m.error);
    expect(m.report.verdict).toBe("pass");
    expect(m.report.checks.map((c) => c.check)).toEqual([...ids]);
    expect(m.report.checks.filter((c) => c.check === "teardown")).toHaveLength(1);
    expect(m.report.checks.find((c) => c.check === "teardown")!.status).toBe("pass");
    expect(validateReport(m.report)).toEqual([]);
    // The transcript gate's backend comes from the run that exercised it.
    expect(m.report.searchGates.transcript.backend).toBe(CH);
    expect(m.report.phases.map((p) => [p.runId, p.reportSha256 ?? null])).toEqual([["aws-proof-run-1", "a".repeat(64)], ["aws-proof-run-2", null]]);
  });
  test("deterministic: the same inputs merge to the same report", () => {
    const a = mergeReports(phase1, "b".repeat(64), phase2);
    const b = mergeReports(structuredClone(phase1), "b".repeat(64), structuredClone(phase2));
    expect(JSON.stringify(a)).toBe(JSON.stringify(b));
  });
  test("refuses a prior for another target, a dry-run prior, a newer prior, or an invalid prior", () => {
    expect(mergeReports({ ...phase1, target: { ...target, accountId: "111111111111" } }, "", phase2)).toEqual({ error: "prior report targets a different accountId" });
    const dry = { ...phase1, mode: "dry-run" as const, phases: [{ ...phase1.phases[0]!, mode: "dry-run" as const }] };
    expect(mergeReports(dry, "", phase2)).toEqual({ error: "a live or read-only run cannot carry dry-run results" });
    expect(mergeReports({ ...phase1, generatedAt: T(59), phases: [{ ...phase1.phases[0]!, generatedAt: T(59) }] }, "", phase2)).toEqual({ error: "prior report is newer than the current run" });
    expect("error" in mergeReports({ ...phase1, verdict: "pass" }, "", phase2)).toBe(true);
  });
  test("a fail carried from phase 1 keeps the merged report failing", () => {
    const failing = buildReport({ runId: "aws-proof-run-1", generatedAt: T(0), mode: "live", target, backends: { metadata: "m", transcript: CH }, checks: [res("backup-restore", "fail", T(0))] });
    const m = mergeReports(failing, "c".repeat(64), phase2);
    expect("report" in m && m.report.verdict).toBe("fail");
  });
  test("--prior-report is a CLI flag", () => {
    expect(parseArgs(["teardown", "--prior-report", "r.json"]).priorReport).toBe("r.json");
  });
});
