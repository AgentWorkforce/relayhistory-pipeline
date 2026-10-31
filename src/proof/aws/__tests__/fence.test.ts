import { describe, expect, test } from "bun:test";
import { parseArgs } from "../cli.ts";
import { defaultConfig, kindReadOnlyConfig, validateConfig, type ProofConfig } from "../config.ts";
import { Runner } from "../exec.ts";
import { fenceFor, PROOF_HOST, PROOF_LOCKER_BUCKET, stepFenceIssue } from "../fence.ts";
import { PROOF_ACCOUNT_ID, PROOF_CLUSTER } from "../guard.ts";
import { buildReport, canonicalReportBytes, mergeReports, sanitize, sha256, validateReport, type CheckResult, type ProofReport } from "../report.ts";

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
    expect(issues({ ingress: { hostname: "sessions-dev.agentrelay.com", burst: {} } }).join("\n")).toMatch(/production- or shared-shaped[\s\S]*ingress.hostname must be "proof-sessions/);
    expect(issues({ ingress: { hostname: "sessions.agentrelay.com", burst: {} } })).toContain(`ingress.hostname must be "${PROOF_HOST}" (got "sessions.agentrelay.com")`);
    expect(issues({ historyOrigin: "https://sessions.agentrelay.com" }).length).toBe(1);
  });
  test("sessions-prod / prod / production names are refused even in the proof account", () => {
    expect(issues({ namespace: "sessions-prod" }).some((i) => i.startsWith("namespace is production"))).toBe(true);
    expect(issues({ release: "sessions-prod" }).some((i) => i.startsWith("release is production"))).toBe(true);
    expect(issues({ coreDeployments: ["96bulls-prod-ingest"] }).some((i) => i.includes("production"))).toBe(true);
    expect(issues({ backupBucket: `96bulls-aws-proof-production-${PROOF_ACCOUNT_ID}` }).some((i) => i.includes("production"))).toBe(true);
  });
  test("buckets, namespaces, releases, deployments, repo and context outside the stage are refused", () => {
    expect(issues({ locker: { bucket: "some-other-bucket", prefix: "" } })).toContain(`locker.bucket must be "${PROOF_LOCKER_BUCKET}" (got "some-other-bucket")`);
    expect(issues({ backupBucket: "shared-backups" })[0]).toMatch(/^backupBucket must be/);
    expect(issues({ namespace: "default" })[0]).toMatch(/^namespace must be "96bulls"/);
    expect(issues({ release: "other" })).toContain('release must be "96bulls" (got "other")');
    expect(issues({ coreDeployments: ["other-ingest"] })[0]).toMatch(/^coreDeployments must be/);
    expect(issues({ chartRepo: "https://evil.example/charts" })[0]).toMatch(/chartRepo must be/);
    expect(issues({ kubeContext: "arn:aws:eks:us-west-2:111111111111:cluster/96bulls-aws-proof" })[0]).toMatch(/kubeContext must be/);
  });
  const fence = fenceFor("proof");
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
    const kf = fenceFor("kind-readonly");
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
    const r = new Runner({ mode: "dry-run", identity, fence: fenceFor("proof"), print: (l) => printed.push(l) });
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

describe("fix 3: the two teardown phases merge into one reproducible, bound report", () => {
  const T = (m: number) => new Date(Date.UTC(2026, 9, 30, 12, m)).toISOString();
  const CH = "ClickHouse Cloud abc.clickhouse.cloud (proof.t; feed: harness-synthetic)";
  const PROOF = { accountId: PROOF_ACCOUNT_ID, region: "us-west-2" };
  const target = { stage: "aws-proof", clusterName: PROOF_CLUSTER, kubeContext: ARN, accountId: PROOF_ACCOUNT_ID, region: "us-west-2" };
  const step = (status: "ran" | "planned") => ({ id: "s", command: "kubectl get pods", mutating: false, touches: [], approval: "operator" as const, status });
  const res = (check: CheckResult["check"], status: CheckResult["status"], at: string, stepStatus: "ran" | "planned" = "ran"): CheckResult => ({ check, status, startedAt: at, finishedAt: at, summary: status, metrics: {}, failures: status === "fail" ? ["x"] : [], evidence: [], steps: [step(stepStatus)] });
  const ids = ["fresh-install", "planted-trace", "public-ingress", "tenant-isolation", "v1-search-scope", "transcript-search", "stale-summary", "restart-persistence", "upgrade-rollback", "backup-restore", "teardown"] as const;
  const phase1 = buildReport({ runId: "aws-proof-run-1", generatedAt: T(1), mode: "live", target, backends: { metadata: "HoopDb Postgres", transcript: CH }, checks: ids.map((id) => res(id, id === "v1-search-scope" ? "unsupported" : id === "teardown" ? "blocked" : "pass", T(0))) });
  const phase2 = buildReport({ runId: "aws-proof-run-2", generatedAt: T(31), mode: "live", target, backends: { metadata: "HoopDb Postgres", transcript: "none" }, checks: [res("teardown", "pass", T(30))] });
  const bytes = (r: unknown) => canonicalReportBytes(r as Parameters<typeof canonicalReportBytes>[0]);
  test("phase 1 alone is incomplete; phase 2 alone is incomplete; merged is PASS and binds the prior's digest", () => {
    expect(phase1.verdict).toBe("incomplete");
    expect(phase2.verdict).toBe("incomplete");
    const m = mergeReports(bytes(phase1), phase2, PROOF);
    if ("error" in m) throw new Error(m.error);
    expect(m.report.verdict).toBe("pass");
    expect(m.report.checks.map((c) => c.check)).toEqual([...ids]);
    expect(m.report.checks.filter((c) => c.check === "teardown")).toHaveLength(1);
    expect(m.report.checks.find((c) => c.check === "teardown")!.status).toBe("pass");
    expect(validateReport(m.report)).toEqual([]);
    expect(m.report.searchGates.transcript.backend).toBe(CH);
    expect(m.report.phases.map((p) => [p.runId, p.reportSha256 ?? null])).toEqual([["aws-proof-run-1", sha256(bytes(phase1))], ["aws-proof-run-2", null]]);
  });
  test("deterministic: the same bytes merge to the same report", () => {
    expect(JSON.stringify(mergeReports(bytes(phase1), phase2, PROOF))).toBe(JSON.stringify(mergeReports(bytes(structuredClone(phase1)), structuredClone(phase2), PROOF)));
  });
  test("review counterexample: forged prior (top-level live, dry-run phase, planned-only PASS checks) is refused", () => {
    const planned = ids.map((id) => res(id, id === "v1-search-scope" ? "unsupported" : id === "teardown" ? "blocked" : "pass", T(0), "planned"));
    const forged = { ...phase1, checks: planned, phases: [{ ...phase1.phases[0]!, mode: "dry-run" as const }] };
    const m = mergeReports(bytes(forged), phase2, PROOF);
    expect("error" in m).toBe(true);
    // A dry-run phase is refused on its own, even when its steps claim to have run.
    const dryPhase = { ...phase1, phases: [{ ...phase1.phases[0]!, mode: "dry-run" as const }] };
    expect("error" in mergeReports(bytes(dryPhase), phase2, PROOF)).toBe(true);
    expect(validateReport(dryPhase)).toContain("phases[0] is dry-run inside a live report");
    // Even with the phase mode patched to live, planned-only passes are refused.
    const patched = { ...phase1, checks: planned };
    expect(mergeReports(bytes(patched), phase2, PROOF)).toMatchObject({ error: expect.stringMatching(/without executed steps/) });
  });
  test("review counterexample: the digest is computed over the exact canonical bytes; edited or reformatted priors are refused", () => {
    expect(mergeReports(JSON.stringify(phase1), phase2, PROOF)).toEqual({ error: "prior report bytes are not the canonical serialization (edited or reformatted)" });
    expect(mergeReports("not json", phase2, PROOF)).toEqual({ error: "prior report is not JSON" });
    const carried = { ...phase1, phases: [{ ...phase1.phases[0]!, reportSha256: "junk" }] };
    expect("error" in mergeReports(bytes(carried), phase2, PROOF)).toBe(true);
  });
  test("refuses a prior inconsistent with the current run: target, region, account, run id, timestamps, dry-run", () => {
    expect(mergeReports(bytes({ ...phase1, target: { ...target, accountId: "111111111111" } }), phase2, PROOF)).toEqual({ error: "prior report targets a different accountId" });
    const eu = { ...target, region: "eu-west-1" };
    expect(mergeReports(bytes({ ...phase1, target: eu }), { ...phase2, target: eu }, PROOF)).toEqual({ error: "reports must target the proof account and region" });
    expect(mergeReports(bytes(phase1), { ...phase2, mode: "dry-run", phases: [{ ...phase2.phases[0]!, mode: "dry-run" }] }, PROOF)).toEqual({ error: "only live reports merge" });
    const early = buildReport({ runId: "aws-proof-run-2", generatedAt: T(31), mode: "live", target, backends: { metadata: "m", transcript: "none" }, checks: [res("teardown", "pass", T(0))] });
    expect(mergeReports(bytes(phase1), early, PROOF)).toMatchObject({ error: expect.stringMatching(/started before the prior report/) });
    expect(mergeReports(bytes({ ...phase1, phases: [{ ...phase1.phases[0]!, runId: "other" }] }), phase2, PROOF)).toMatchObject({ error: expect.stringMatching(/phases must end with this run|prior phase does not match/) });
    const merged = mergeReports(bytes(phase1), phase2, PROOF) as { report: ProofReport };
    expect(mergeReports(bytes(merged.report), { ...phase2, runId: "aws-proof-run-3", phases: [{ ...phase2.phases[0]!, runId: "aws-proof-run-3", generatedAt: T(59) }], generatedAt: T(59) }, PROOF)).toEqual({ error: "prior report must be a single unmerged phase" });
  });
  test("a fail carried from phase 1 keeps the merged report failing", () => {
    const failing = buildReport({ runId: "aws-proof-run-1", generatedAt: T(1), mode: "live", target, backends: { metadata: "m", transcript: CH }, checks: [res("backup-restore", "fail", T(0))] });
    const m = mergeReports(bytes(failing), phase2, PROOF);
    expect("report" in m && m.report.verdict).toBe("fail");
  });
  test("--prior-report is a CLI flag", () => {
    expect(parseArgs(["teardown", "--prior-report", "r.json"]).priorReport).toBe("r.json");
  });
});

describe("re-review counterexamples: the fence is hardcoded, not derived from config", () => {
  const f = fenceFor("proof");
  test("an EKS ARN in the right account but eu-west-1 is refused (config and step)", () => {
    const eu = `arn:aws:eks:eu-west-1:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`;
    expect(issues({ kubeContext: eu }).some((i) => i.startsWith("kubeContext must be"))).toBe(true);
    expect(issues({ region: "eu-west-1" }).some((i) => i.startsWith("region must be"))).toBe(true);
    expect(stepFenceIssue(["kubectl", "--context", eu, "-n", "96bulls", "get", "pods"], f)).toBe("context_outside_fence");
    expect(stepFenceIssue(["aws", "wafv2", "get-web-acl-for-resource", "--region", "us-west-2", "--resource-arn", `arn:aws:elasticloadbalancing:eu-west-1:${PROOF_ACCOUNT_ID}:loadbalancer/app/x/y`], f)).toBe("arn_region_outside_fence");
    expect(stepFenceIssue(["sh", "-c", `aws s3 cp --region eu-west-1 s3://${PROOF_LOCKER_BUCKET}/k -`], f)).toBe("region_outside_fence");
    expect(stepFenceIssue(["aws", "s3api", "list-objects-v2", "--bucket", PROOF_LOCKER_BUCKET], f)).toBe("region_missing");
  });
  test("swapped namespaces, arbitrary services/deployments/database and attacker transcript host are refused", () => {
    expect(issues({ namespace: "relayauth", relayauth: { namespace: "96bulls", service: "relayauth", port: 8787 } }).length).toBeGreaterThanOrEqual(2);
    expect(issues({ coreDeployments: ["96bulls-arbitrary"] })[0]).toMatch(/^coreDeployments must be/);
    expect(issues({ relayauth: { namespace: "relayauth", service: "anything", port: 8787 } })[0]).toMatch(/^relayauth must be/);
    expect(issues({ relaycast: { namespace: "relaycast", service: "anything", port: 8787 } })[0]).toMatch(/^relaycast must be/);
    expect(issues({ hoopdb: { cluster: "hoopdb", database: "postgres" } })[0]).toMatch(/^hoopdb must be/);
    expect(issues({ ingest: { service: "96bulls-arbitrary", port: 8080 } })[0]).toMatch(/^ingest.service must be/);
    const t = { ...defaultConfig().transcriptSearch, backend: "clickhouse" as const, endpoint: "https://attacker.example", database: "proof_victim", table: "t" };
    const ti = issues({ transcriptSearch: t });
    expect(ti.some((i) => i.includes("attacker.example is not an allowlisted"))).toBe(true);
    expect(ti.some((i) => i.startsWith("transcriptSearch database/table must be"))).toBe(true);
    // The step fence does not consult config either.
    expect(stepFenceIssue(["kubectl", "--context", ARN, "-n", "96bulls", "rollout", "restart", "deployment/96bulls-arbitrary"], f)).toBe("deployment_outside_fence");
    expect(stepFenceIssue(["kubectl", "--context", ARN, "-n", "relayauth", "port-forward", "svc/anything", "1:2"], f)).toBe("service_outside_fence");
    expect(stepFenceIssue(["kubectl", "--context", ARN, "-n", "96bulls", "exec", "-i", "hoopdb-1", "-c", "postgres", "--", "psql", "-d", "postgres", "-f", "-"], f)).toBe("database_outside_fence");
    expect(stepFenceIssue(["kubectl", "--context", ARN, "-n", "96bulls", "get", "clusters.postgresql.cnpg.io", "other"], f)).toBe("pg_cluster_outside_fence");
    expect(stepFenceIssue(["HTTP", "POST", "https://attacker.example/?query=x"], f)).toBe("host_outside_fence");
    expect(stepFenceIssue(["helm", "--kube-context", ARN, "-n", "96bulls", "uninstall", "other"], f)).toBe("release_outside_fence");
  });
  test("-A / --all-namespaces is refused for every command; a delete must name one writable namespace", () => {
    expect(stepFenceIssue(["kubectl", "--context", ARN, "delete", "pods", "-A", "-l", "96bulls.proof=true"], f)).toBe("all_namespaces_refused");
    expect(stepFenceIssue(["kubectl", "--context", ARN, "get", "pods", "--all-namespaces"], f)).toBe("all_namespaces_refused");
    expect(stepFenceIssue(["kubectl", "--context", ARN, "delete", "pods", "-l", "x=y"], f)).toBe("delete_needs_allowlisted_namespace");
    expect(stepFenceIssue(["kubectl", "--context", ARN, "-n", "relayauth", "delete", "pod", "x"], f)).toBe("delete_needs_allowlisted_namespace");
    expect(stepFenceIssue(["kubectl", "--context", ARN, "delete", "namespace", "relayauth"], f)).toBe("delete_namespace_outside_fence");
    expect(stepFenceIssue(["kubectl", "--context", ARN, "-n", "96bulls", "delete", "pod", "x"], f)).toBeUndefined();
    expect(stepFenceIssue(["kubectl", "--context", ARN, "delete", "namespace", "96bulls"], f)).toBeUndefined();
  });
  test("every planned step of every check stays inside the fence (no -A anywhere)", async () => {
    const { teardownSteps, teardownVerifySteps, backupRestoreSteps, restoreCleanupSteps, rolloutRestartSteps, failoverSteps, relaycastProbeSteps, upgradeRollbackSteps } = await import("../lifecycle.ts");
    const c = { ...defaultConfig(), candidateChartVersion: "0.3.1", probeImage: `ghcr.io/x@sha256:${"a".repeat(64)}` };
    const all = [...teardownSteps(c), ...teardownVerifySteps(c), ...backupRestoreSteps(c, "proof-x"), ...restoreCleanupSteps(c, "proof-x"), ...rolloutRestartSteps(c), ...failoverSteps(c, "hoopdb-1"),
      ...relaycastProbeSteps(c, "start"), ...relaycastProbeSteps(c, "before"), ...relaycastProbeSteps(c, "after"), ...relaycastProbeSteps(c, "delete"), ...upgradeRollbackSteps(c, "3")];
    for (const s of all) expect([s.id, stepFenceIssue(s.argv, f)]).toEqual([s.id, undefined]);
  });
});
