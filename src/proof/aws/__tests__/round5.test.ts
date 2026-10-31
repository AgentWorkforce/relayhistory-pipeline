/**
 * #74 round 5 (final consolidated set at ed4c0a0) and the #75 endpoint/tenant
 * items: each reproduced.
 */
import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PROV } from "./prov.ts";
import { newCtx, postDestroyTeardown, SEARCH_CLIENT_SOURCE, searchClientSteps, unlistedOrgProbe } from "../checks.ts";
import { parseArgs } from "../cli.ts";
import { credentialIssue, parseDescribeCluster, PROOF_CLUSTER_ARN, proofKubeconfig, toolEnv } from "../cluster-binding.ts";
import { defaultConfig, validateConfig } from "../config.ts";
import { plantCorpus } from "../corpus.ts";
import { Runner, type Spawn } from "../exec.ts";
import { harnessOutputIssues, EXPECTED_HARNESS_OUTPUTS, PROOF_CONTEXT, PROOF_FENCE, PROOF_PROBE_IMAGES, PROOF_TENANT_ORGS } from "../fence.ts";
import { assertMayRun, GuardError, PROOF_ACCOUNT_ID, PROOF_CLUSTER } from "../guard.ts";
import { locationOf } from "../ingest.ts";
import { admitStep, probePod, stepFenceIssue } from "../ops.ts";
import { bindStagePlan, parsePlanOps, planOpsIssues } from "../provenance.ts";
import { buildReport, CHECK_IDS, finalVerdict, validateReport, type CheckResult } from "../report.ts";
import { manifestKey } from "../../../shared/vendor.ts";

const ARN = PROOF_CONTEXT;
const k = (...a: string[]) => ["kubectl", "--context", ARN, ...a];
const identity = { confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, kubeContextCluster: ARN, clusterBound: true };

describe("(1) the stage plan must be create/noop only", () => {
  // The exact shape of the #75 continuation capture at ecf70fd, trimmed: 4 create, 1 replace of a never-created record.
  const ecf70fd = ["Plan · aws-proof", "Importing stack module", "Resolving stack services", "AWS: using environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION) instead of the profile.", "Loading stack state", "Computing plan", "Plan ready (4.2s)", "Plan: 4 to create, 1 to replace", "[96bulls] noop", "[cluster] noop", "[ingest-cert-validation] replace", "[ingest-ingress] create", "[ingest-waf] create", "[netpol-a] create", "[netpol-b] create"].join("\n");
  test("the ecf70fd continuation (1 replace) is rejected even though [cluster] is present", () => {
    const ops = parsePlanOps(ecf70fd);
    if ("error" in ops) throw new Error(ops.error);
    expect(planOpsIssues(ops)).toEqual(["plan would replace 1 resource(s)", "plan would replace [ingest-cert-validation]"]);
  });
  test("update / delete / import / adopt are each rejected", () => {
    for (const op of ["update", "delete", "import", "adopt"]) {
      const ops = parsePlanOps(`Plan: 1 to create, 1 to ${op}\n[cluster] create\n[locker] ${op}\n`);
      if ("error" in ops) throw new Error(ops.error);
      expect(planOpsIssues(ops).length).toBeGreaterThan(0);
    }
  });
  test("summary and listing must agree; unknown lines and missing [cluster] fail closed", () => {
    const ops = parsePlanOps("Plan: 3 to create\n[cluster] create\n[locker] create\n") as Exclude<ReturnType<typeof parsePlanOps>, { error: string }>;
    expect(planOpsIssues(ops)).toEqual(["plan summary says 3 to create but lists 2"]);
    expect(parsePlanOps("Plan: 1 to create\n~ something else\n[cluster] create\n")).toMatchObject({ error: expect.stringMatching(/unrecognized plan line/) });
    expect(planOpsIssues(parsePlanOps("Plan: 1 to create\n[locker] create\n") as never)).toContain("plan has no [cluster] resource");
  });
  test("a reviewed update is accepted only when passed by --allow-plan-update, never a replace", () => {
    const two = parsePlanOps("Plan: 2 to update\n[cluster] noop\n[96bulls] update\n[alleyoop-role] update\n") as Exclude<ReturnType<typeof parsePlanOps>, { error: string }>;
    expect(planOpsIssues(two)).toEqual(["plan would update [96bulls]", "plan would update [alleyoop-role]"]);
    expect(planOpsIssues(two, ["96bulls", "alleyoop-role"])).toEqual([]);
    expect(planOpsIssues(two, ["96bulls"])).toEqual(["plan would update [alleyoop-role]"]);
    expect(planOpsIssues(two, ["locker"])[0]).toBe("--allow-plan-update locker is not a reviewed update");
    const rep = parsePlanOps("Plan: 1 to replace\n[cluster] noop\n[96bulls] replace\n") as Exclude<ReturnType<typeof parsePlanOps>, { error: string }>;
    expect(planOpsIssues(rep, ["96bulls"]).length).toBeGreaterThan(0);
    expect(parseArgs(["fresh-install", "--allow-plan-update", "96bulls", "--allow-plan-update", "alleyoop-role"]).allowPlanUpdates).toEqual(["96bulls", "alleyoop-role"]);
  });
  test("a noop cluster (continuation plan) must already have existed at capture", () => {
    const d = parseDescribeCluster(JSON.stringify({ cluster: { arn: PROOF_CLUSTER_ARN, name: PROOF_CLUSTER, status: "ACTIVE", endpoint: "https://ABC.gr7.us-west-2.eks.amazonaws.com", createdAt: "2026-10-31T02:00:00.000Z", certificateAuthority: { data: "Y2E=" } } })) as Exclude<ReturnType<typeof parseDescribeCluster>, { error: string }>;
    const plan = { generatedAt: "2026-10-31T04:58:37.391Z", clusterOp: "noop" } as Parameters<typeof bindStagePlan>[0];
    expect(bindStagePlan(plan, d)).toEqual({ ok: true });
    expect(bindStagePlan({ ...plan, generatedAt: "2026-10-31T01:00:00.000Z" }, d)).toMatchObject({ error: expect.stringMatching(/created after the capture/) });
  });
});

describe("(2) every tool gets a minimal, explicit environment", () => {
  const hostile = {
    PATH: "/usr/bin:/bin", HOME: "/home/op", AWS_ACCESS_KEY_ID: "AKIAEXAMPLEEXAMPLE00", AWS_SECRET_ACCESS_KEY: "s".repeat(40), AWS_SESSION_TOKEN: "tok",
    AWS_CONFIG_FILE: "/home/op/.aws/evil", AWS_SHARED_CREDENTIALS_FILE: "/home/op/.aws/evil-creds", AWS_PROFILE: "prod-admin", AWS_DEFAULT_PROFILE: "prod-admin",
    AWS_ENDPOINT_URL: "https://attacker.example", AWS_ENDPOINT_URL_STS: "https://attacker.example", AWS_ENDPOINT_URL_EKS: "https://attacker.example",
    AWS_CA_BUNDLE: "/tmp/evil.pem", AWS_STS_REGIONAL_ENDPOINTS: "legacy", AWS_USE_FIPS_ENDPOINT: "true", AWS_USE_DUALSTACK_ENDPOINT: "true", AWS_REGION: "eu-west-1",
    AWS_ROLE_ARN: "arn:aws:iam::111111111111:role/x", AWS_WEB_IDENTITY_TOKEN_FILE: "/tmp/t", HTTPS_PROXY: "http://attacker.example:8080", KUBECONFIG: "/home/op/.kube/config",
    HELM_KUBEAPISERVER: "https://attacker.example", HELM_KUBETOKEN: "t", KUBECTL_EXTERNAL_DIFF: "sh",
  };
  test("only the explicit keys survive; retargeting variables are dropped and the pins are forced", () => {
    const env = toolEnv(hostile, "/run/k.json");
    expect(Object.keys(env).sort()).toEqual([
      "AWS_ACCESS_KEY_ID", "AWS_CONFIG_FILE", "AWS_DEFAULT_REGION", "AWS_EC2_METADATA_DISABLED", "AWS_PAGER", "AWS_REGION", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
      "AWS_SHARED_CREDENTIALS_FILE", "AWS_STS_REGIONAL_ENDPOINTS", "AWS_USE_DUALSTACK_ENDPOINT", "AWS_USE_FIPS_ENDPOINT", "HOME", "KUBECONFIG", "PATH",
    ]);
    expect(env).toMatchObject({ AWS_CONFIG_FILE: "/dev/null", AWS_SHARED_CREDENTIALS_FILE: "/dev/null", AWS_REGION: "us-west-2", AWS_STS_REGIONAL_ENDPOINTS: "regional", AWS_USE_FIPS_ENDPOINT: "false", KUBECONFIG: "/run/k.json" });
  });
  test("credentials must be explicit env vars", () => {
    expect(credentialIssue({ AWS_PROFILE: "x" })).toMatch(/must be explicit env vars/);
    expect(credentialIssue({ AWS_ACCESS_KEY_ID: "a", AWS_SECRET_ACCESS_KEY: "b" })).toBeUndefined();
  });
  test("the kubeconfig exec plugin repeats the config-file and region pins", () => {
    const d = { arn: PROOF_CLUSTER_ARN, endpoint: "https://ABC.gr7.us-west-2.eks.amazonaws.com", caData: "Y2E=", createdAtMs: 0 };
    const env = JSON.parse(proofKubeconfig(d, ARN)).users[0].user.exec.env as Array<{ name: string; value: string }>;
    expect(Object.fromEntries(env.map((e) => [e.name, e.value]))).toEqual({ AWS_CONFIG_FILE: "/dev/null", AWS_SHARED_CREDENTIALS_FILE: "/dev/null", AWS_STS_REGIONAL_ENDPOINTS: "regional", AWS_REGION: "us-west-2" });
  });
  const aws = spawnSync("aws", ["--version"], { encoding: "utf8" });
  test.skipIf(aws.status !== 0)("adversarial AWS config: the real CLI sees its endpoint_url under the operator env, and nothing under toolEnv (local only)", () => {
    const dir = mkdtempSync(join(tmpdir(), "aws-proof-cfg-"));
    const cfgPath = join(dir, "config");
    writeFileSync(cfgPath, "[default]\nendpoint_url = https://attacker.example\nregion = eu-west-1\n[profile evil]\nendpoint_url = https://attacker.example\n");
    const raw = { ...hostile, PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: dir, AWS_CONFIG_FILE: cfgPath, AWS_PROFILE: "evil", AWS_SHARED_CREDENTIALS_FILE: join(dir, "none") };
    const get = (env: Record<string, string>, key: string) => spawnSync("aws", ["configure", "get", key], { encoding: "utf8", env });
    expect(get(raw as Record<string, string>, "endpoint_url").stdout.trim()).toBe("https://attacker.example");
    const safe = get(toolEnv(raw, undefined), "endpoint_url");
    expect(safe.stdout.trim()).toBe("");
    expect(get(toolEnv(raw, undefined), "region").stdout.trim()).toBe("");
  });
});

describe("(3) post-destroy absence is verifiable without a live cluster", () => {
  const cfg = defaultConfig();
  const fake = (o: { describe?: [number, string]; list?: string; tagged?: string; bucket?: [number, string] } = {}): Spawn => (argv) => {
    if (argv[2] === "describe-cluster") return { stdout: "", status: o.describe?.[0] ?? 254, stderr: o.describe?.[1] ?? "An error occurred (ResourceNotFoundException) when calling the DescribeCluster operation: No cluster found" };
    if (argv[2] === "list-clusters") return { stdout: o.list ?? "[]", status: 0 };
    if (argv[1] === "resourcegroupstaggingapi") return { stdout: o.tagged ?? JSON.stringify({ ResourceTagMappingList: [] }), status: 0 };
    if (argv[2] === "head-bucket") return { stdout: "", status: o.bucket?.[0] ?? 254, stderr: o.bucket?.[1] ?? "An error occurred (404) when calling the HeadBucket operation: Not Found" };
    return { stdout: "", status: 1 };
  };
  const run = async (spawn: Spawn) => {
    const r = new Runner({ mode: "live", identity: { confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, postDestroy: true }, fence: PROOF_FENCE, awsOnly: true, spawn, print: () => {} });
    const ctx = newCtx({ ...cfg, sessions: 2 }, r, "aws-proof-post-0001", {});
    ctx.print = () => {};
    return postDestroyTeardown(ctx);
  };
  test("everything gone: PASS, with no kube context or describe preflight", async () => {
    const res = await run(fake());
    expect(res.status).toBe("pass");
    expect(res.steps.every((s) => s.command.startsWith("aws "))).toBe(true);
  });
  test("an AccessDenied describe, an existing bucket, a listed cluster or a tagged leftover is FAIL, never 'gone'", async () => {
    expect((await run(fake({ describe: [254, "An error occurred (AccessDeniedException)"] }))).failures).toContain("describe-cluster failed without ResourceNotFound; absence unproven");
    expect((await run(fake({ bucket: [0, ""] }))).failures.some((f) => /still exists/.test(f))).toBe(true);
    expect((await run(fake({ bucket: [254, "An error occurred (403) when calling the HeadBucket operation: Forbidden"] }))).failures.some((f) => /absence unproven/.test(f))).toBe(true);
    expect((await run(fake({ list: JSON.stringify([PROOF_CLUSTER]) }))).failures).toContain("list-clusters still names the cluster");
    expect((await run(fake({ tagged: JSON.stringify({ ResourceTagMappingList: [{ ResourceARN: "arn:aws:s3:::96bulls-aws-proof-locker-500268966557" }] }) }))).status).toBe("fail");
  });
  test("the post-destroy runner is aws-only and read-only", () => {
    const r = new Runner({ mode: "live", identity: { confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, postDestroy: true }, fence: PROOF_FENCE, awsOnly: true, spawn: () => ({ stdout: "", status: 0 }), print: () => {} });
    expect(r.run({ id: "x", argv: k("-n", "96bulls", "get", "pods"), mutating: false, touches: [], approval: "operator" }).record.code).toBe("post_destroy_aws_only");
    let code = "";
    try {
      assertMayRun({ mode: "live", mutating: true, confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, postDestroy: true });
    } catch (e) {
      code = (e as GuardError).code;
    }
    expect(code).toBe("post_destroy_read_only");
  });
  test("--post-destroy needs the prior report and runs teardown only", () => {
    expect(() => parseArgs(["teardown", "--post-destroy", "--confirm-live"])).toThrow(/needs --prior-report/);
    expect(() => parseArgs(["fresh-install", "--post-destroy", "--confirm-live", "--prior-report", "r"])).toThrow(/teardown check only/);
    expect(parseArgs(["teardown", "--post-destroy", "--confirm-live", "--prior-report", "r"]).postDestroy).toBe(true);
  });
});

describe("(4) transcript evidence and deferral", () => {
  const T = "2026-10-31T03:00:00.000Z";
  const ran = [{ id: "s", command: "x", mutating: false, touches: [], approval: "operator" as const, status: "ran" as const }];
  const all = (transcript: CheckResult["status"], summary = "ok") => CHECK_IDS.map((id): CheckResult => ({ check: id, status: id === "v1-search-scope" ? "unsupported" : id === "transcript-search" ? transcript : "pass", startedAt: T, finishedAt: T, summary: id === "transcript-search" ? summary : "ok", metrics: {}, failures: [], evidence: [], steps: id === "transcript-search" && transcript === "deferred" ? [] : ran }));
  test("a deferred transcript gate does not block the verdict and is never PASS", () => {
    const checks = all("deferred", "transcript-search deferred by owner 2026-10-31; not run");
    expect(finalVerdict(checks, "none", PROV).verdict).toBe("pass");
    const r = buildReport({ provenance: PROV, runId: "x", generatedAt: "2026-10-31T03:00:01.000Z", mode: "live", target: { stage: "aws-proof", clusterName: PROOF_CLUSTER, kubeContext: ARN }, backends: { metadata: "m", transcript: "none" }, checks });
    expect(r.checks.find((c) => c.check === "transcript-search")!.status).toBe("deferred");
    expect(r.searchGates.transcript.deferred).toBe("deferred by owner 2026-10-31");
    expect(validateReport(r)).toEqual([]);
  });
  test("a blocked (e.g. synthetic) transcript gate keeps the proof incomplete; only transcript-search may be deferred", () => {
    expect(finalVerdict(all("blocked"), "none", PROV).verdict).toBe("incomplete");
    const bad = all("deferred", "deferred by owner 2026-10-31").map((c) => (c.check === "backup-restore" ? { ...c, status: "deferred" as const, summary: "deferred by owner 2026-10-31", steps: [] } : c));
    const r = buildReport({ provenance: PROV, runId: "x", generatedAt: "2026-10-31T03:00:01.000Z", mode: "live", target: { stage: "aws-proof", clusterName: PROOF_CLUSTER, kubeContext: ARN }, backends: { metadata: "m", transcript: "none" }, checks: bad });
    expect(validateReport(r).some((i) => i.includes("only transcript-search may be deferred"))).toBe(true);
    expect(r.verdict).toBe("incomplete");
  });
  test("the V1 metadata search gate stays mandatory", () => {
    const checks = all("deferred", "deferred by owner 2026-10-31").map((c) => (c.check === "planted-trace" ? { ...c, status: "blocked" as const } : c));
    expect(finalVerdict(checks, "none", PROV).verdict).toBe("incomplete");
  });
});

describe("(5) the search exchange goes to RelayAuth TLS from a labelled in-cluster client", () => {
  const cfg = { ...defaultConfig(), probeImage: PROOF_PROBE_IMAGES[0]! };
  test("the client script calls relayauth-tls:8443 with the CA, never plain :8787, and PostgREST in-cluster", () => {
    expect(SEARCH_CLIENT_SOURCE).toContain("https://relayauth-tls.relayauth.svc:8443/v1/tokens/session-directory-search");
    expect(SEARCH_CLIENT_SOURCE).toContain("tls: { ca }");
    expect(SEARCH_CLIENT_SOURCE).toContain("http://session-directory-postgrest.96bulls.svc:3000/rpc/search_session_directory");
    expect(SEARCH_CLIENT_SOURCE).not.toContain(":8787");
  });
  test("the client pod carries both caller labels and mounts relayauth-ca, and is admitted exactly", () => {
    const spec = probePod("search", PROOF_PROBE_IMAGES[0]!);
    expect(spec.labels).toBe("96bulls.proof=true,relayauth.agentrelay.com/client=tls,96bulls.agentrelay.com/search-client=true");
    expect(JSON.parse(spec.overrides).spec.volumes).toEqual([{ name: "ca", configMap: { name: "relayauth-ca" } }]);
    for (const s of searchClientSteps(cfg)) expect([s.id, admitStep(s, PROOF_FENCE)]).toEqual([s.id, expect.objectContaining({ ok: true, mutating: s.mutating })]);
    expect(admitStep({ argv: k("-n", "96bulls", "exec", "-i", "proof-search-client", "--", "bun", "run", "-") }, PROOF_FENCE)).toMatchObject({ ok: true, mutating: false });
  });
  test("a tampered override (hostPath volume, privileged) or other labels is refused", () => {
    const spec = probePod("search", PROOF_PROBE_IMAGES[0]!);
    const run = (labels: string, overrides: string) => k("-n", "96bulls", "run", "proof-search-client", `--image=${PROOF_PROBE_IMAGES[0]}`, "--restart=Never", `--labels=${labels}`, "--overrides", overrides, "--command", "--", "sleep", "7200");
    const evil = JSON.parse(spec.overrides);
    evil.spec.volumes.push({ name: "host", hostPath: { path: "/" } });
    expect(stepFenceIssue(run(spec.labels, JSON.stringify(evil)), PROOF_FENCE)).toBe("probe_overrides_not_allowlisted");
    const priv = JSON.parse(spec.overrides);
    priv.spec.containers[0].securityContext = { privileged: true };
    expect(stepFenceIssue(run(spec.labels, JSON.stringify(priv)), PROOF_FENCE)).toBe("probe_overrides_not_allowlisted");
    expect(stepFenceIssue(run("96bulls.proof=true,app=relayauth", spec.overrides), PROOF_FENCE)).toBe("probe_labels_not_allowlisted");
  });
  test("the exchange key reaches the pod on stdin only; the report never sees it", async () => {
    const seen: Array<{ argv: string[]; stdin?: string }> = [];
    const spawn: Spawn = (argv, stdin) => {
      seen.push({ argv, stdin });
      if (argv.includes("exec")) return { stdout: JSON.stringify({ tag: "q1", rows: [{ session_id: "s", org_id: "org_aws_proof_1", short_summary: null }], ms: 3 }), status: 0 };
      return { stdout: "", status: 0 };
    };
    const ctx = newCtx({ ...cfg, sessions: 2 }, new Runner({ mode: "live", identity, fence: PROOF_FENCE, spawn, print: () => {}, kubeconfigPath: "/run/k.json" }), "aws-proof-tls-0001", { PROOF_SEARCH_EXCHANGE_KEY: "rak_exchange_key_123456" });
    ctx.print = () => {};
    ctx.tokens.set("org_aws_proof_1/default", "subject-token-abcdef");
    const { v1SearchScope } = await import("../checks.ts");
    ctx.searchable = new Set(ctx.corpus.map((p) => p.sessionId));
    const res = await v1SearchScope(ctx);
    const exec = seen.find((s) => s.argv.includes("exec") && s.argv.includes("proof-search-client"))!;
    expect(exec.stdin).toContain("rak_exchange_key_123456");
    expect(exec.argv.join(" ")).not.toContain("rak_exchange_key_123456");
    expect(JSON.stringify(res)).not.toContain("rak_exchange_key_123456");
    expect(JSON.stringify(res)).not.toContain("subject-token-abcdef");
  });
});

describe("(6) two proof tenants, per-org key prefixes, and the unlisted-org refusal", () => {
  test("defaults are the two #75 tenants; any other tenant set is refused", () => {
    expect(defaultConfig().tenants.map((t) => t.orgId)).toEqual([...PROOF_TENANT_ORGS]);
    expect(validateConfig({ ...defaultConfig(), tenants: [{ orgId: "org_aws_proof_1", workspaceId: "default" }, { orgId: "org_other", workspaceId: "default" }] })[0]).toMatch(/^tenants must be/);
  });
  test("planted keys land under 96bulls/<org>/v1/...", () => {
    const [a, b] = plantCorpus({ runId: "aws-proof-prefix-0001", sessions: 2, tenants: defaultConfig().tenants, keyPrefixBase: "96bulls" });
    expect(manifestKey(locationOf(a!))).toMatch(/^96bulls\/org_aws_proof_1\/v1\/ws=default\//);
    expect(manifestKey(locationOf(b!))).toMatch(/^96bulls\/org_aws_proof_2\/v1\/ws=default\//);
    const del = ["aws", "s3api", "delete-object", "--region", "us-west-2", "--bucket", "96bulls-aws-proof-locker-500268966557", "--key", manifestKey(locationOf(a!)).replace(/manifest\.json$/, "summary/short.json.enc")];
    expect(stepFenceIssue(del, PROOF_FENCE)).toBeUndefined();
    expect(stepFenceIssue(del.map((x) => x.replace("org_aws_proof_1", "org_other")), PROOF_FENCE)).toBe("aws_operation_not_allowlisted");
  });
  test("an unlisted org must get 403 org_not_allowed; the probe sends no bytes", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const f = async (url: string, init?: RequestInit) => {
      calls.push({ url, init });
      return url.includes("relayhistory-assertion") ? new Response(JSON.stringify({ accessToken: "unlisted-token-xyz" }), { status: 201 }) : new Response(JSON.stringify({ error: { code: "org_not_allowed" } }), { status: 403 });
    };
    const ctx = newCtx(defaultConfig(), new Runner({ mode: "live", identity, fence: PROOF_FENCE, print: () => {}, kubeconfigPath: "/run/k.json" }), "aws-proof-unlisted-0001", { PROOF_RELAYAUTH_BOOTSTRAP_KEY: "rak_bootstrap_123456" });
    ctx.print = () => {};
    expect(await unlistedOrgProbe(ctx, "http://127.0.0.1:18787", "http://127.0.0.1:18080", f)).toEqual({ status: 403, code: "org_not_allowed" });
    expect(JSON.parse(String(calls[0]!.init?.body)).orgId).toBe("org_aws_proof_unlisted");
    expect((calls[1]!.init?.body as Uint8Array).byteLength).toBe(0);
    const accepted = async (url: string) => (url.includes("relayhistory-assertion") ? new Response(JSON.stringify({ accessToken: "t" }), { status: 201 }) : new Response("{}", { status: 400 }));
    expect(await unlistedOrgProbe(ctx, "http://127.0.0.1:18787", "http://127.0.0.1:18080", accepted)).toEqual({ status: 400, code: "http_400" });
  });
  test("eks.harness outputs must equal the hardcoded fence", () => {
    const ok = JSON.stringify({ eks: { harness: EXPECTED_HARNESS_OUTPUTS } });
    expect(harnessOutputIssues(ok)).toEqual([]);
    expect(harnessOutputIssues(JSON.stringify({ eks: { harness: { ...EXPECTED_HARNESS_OUTPUTS, relayauthExchangeUrl: "http://relayauth.relayauth.svc:8787" } } }))[0]).toMatch(/relayauthExchangeUrl/);
    expect(harnessOutputIssues(JSON.stringify({ eks: { harness: { ...EXPECTED_HARNESS_OUTPUTS, tenantOrgIds: ["org_aws_proof_1", "org_x"] } } }))[0]).toMatch(/tenantOrgIds/);
    expect(harnessOutputIssues("{}")).toEqual(["stack outputs have no eks.harness"]);
  });
});

describe("phase-1 limitation is stated", () => {
  test("every report carries the NetworkPolicy limitation and validation requires it", () => {
    const r = buildReport({ provenance: PROV, runId: "x", generatedAt: "2026-10-31T03:00:01.000Z", mode: "dry-run", target: { stage: "aws-proof", clusterName: PROOF_CLUSTER, kubeContext: ARN }, backends: { metadata: "m", transcript: "none" }, checks: [] });
    expect(r.limitations.join(" ")).toMatch(/NetworkPolicy is not enforced in phase 1/);
    expect(validateReport({ ...r, limitations: [] })).toContain("limitations must state every phase-1 limitation");
  });
});
