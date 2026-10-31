/**
 * #74 review round 4 (at 9b4fc6b) and the additional HOLD items: each bypass
 * reproduced exactly.
 */
import { describe, expect, test } from "bun:test";
import { PROV } from "./prov.ts";
import { newCtx, v1SearchScope } from "../checks.ts";
import { main, observeIdentity } from "../cli.ts";
import { bindKubeconfig, parseDescribeCluster, PROOF_CLUSTER_ARN, proofKubeconfig, toolEnv } from "../cluster-binding.ts";
import { defaultConfig, validateConfig } from "../config.ts";
import { Runner, type Spawn } from "../exec.ts";
import { PROOF_CONTEXT, PROOF_FENCE, PROOF_PROBE_IMAGES } from "../fence.ts";
import { assertProofTarget, GuardError, PROOF_ACCOUNT_ID, PROOF_CLUSTER } from "../guard.ts";
import { probePod, stepFenceIssue } from "../ops.ts";
import { bindStagePlan, chartDigests, gitProvenance, loadStagePlan, planImageIssues, provenanceGaps, sha256Hex, STAGE_PLAN_FILES } from "../provenance.ts";
import { buildReport, CHECK_IDS, finalVerdict, validateReport, type CheckResult } from "../report.ts";
import { exchangeSearchToken, postgrestSearch } from "../search-http.ts";

const ARN = PROOF_CONTEXT;
const k = (...a: string[]) => ["kubectl", "--context", ARN, ...a];
const CA = Buffer.from("proof-ca").toString("base64");
const ENDPOINT = "https://ABCDEF0123456789.gr7.us-west-2.eks.amazonaws.com";
const describe_ = (over: Record<string, unknown> = {}) => JSON.stringify({ cluster: { arn: PROOF_CLUSTER_ARN, name: PROOF_CLUSTER, status: "ACTIVE", endpoint: ENDPOINT, createdAt: "2026-10-31T02:00:00.000Z", certificateAuthority: { data: CA }, ...over } });
const view = (cluster: Record<string, unknown> = { server: ENDPOINT, "certificate-authority-data": CA }, ctxName = ARN, clusterName = "whatever-the-operator-named-it") =>
  JSON.stringify({ contexts: [{ name: ctxName, context: { cluster: clusterName } }], clusters: [{ name: clusterName, cluster }] });

describe("(1) flags are normalized; repeats and retargeting flags are refused in any spelling", () => {
  test("the reviewed last-wins forms are refused", () => {
    expect(stepFenceIssue(["kubectl", "--context", ARN, "--context=attacker", "-n", "96bulls", "get", "pods"], PROOF_FENCE)).toBe("flag_repeated");
    expect(stepFenceIssue(k("-n", "96bulls", "-n=attacker", "get", "pods"), PROOF_FENCE)).toBe("flag_repeated");
    expect(stepFenceIssue(k("-n", "96bulls", "--namespace", "attacker", "get", "pods"), PROOF_FENCE)).toBe("flag_repeated");
    expect(stepFenceIssue(["helm", "--kube-context", ARN, "--kube-context=attacker", "-n", "96bulls", "list", "-o", "json"], PROOF_FENCE)).toBe("flag_repeated");
    expect(stepFenceIssue(["aws", "eks", "list-clusters", "--region", "us-west-2", "--region=eu-west-1"], PROOF_FENCE)).toBe("flag_repeated");
  });
  test("equivalent spellings normalize to one flag and are checked by value", () => {
    for (const ns of [["-n", "96bulls"], ["-n96bulls"], ["-n=96bulls"], ["--namespace", "96bulls"], ["--namespace=96bulls"]]) expect(stepFenceIssue(k(...ns, "get", "pods"), PROOF_FENCE)).toBeUndefined();
    for (const ns of [["-nattacker"], ["-n=attacker"], ["--namespace=attacker"], ["--namespace", "kube-system"]]) expect(stepFenceIssue(k(...ns, "get", "pods"), PROOF_FENCE)).toBe("namespace_outside_fence");
    expect(stepFenceIssue(["kubectl", `--context=${ARN}`, "-n", "96bulls", "get", "pods"], PROOF_FENCE)).toBeUndefined();
    expect(stepFenceIssue(["kubectl", "--context=attacker", "-n", "96bulls", "get", "pods"], PROOF_FENCE)).toBe("context_outside_fence");
    expect(stepFenceIssue(["aws", "eks", "list-clusters", "--region=eu-west-1"], PROOF_FENCE)).toBe("region_outside_fence");
  });
  test("retargeting flags are refused outright, in every spelling and tool", () => {
    const kube = [["--server", "https://x"], ["--server=https://x"], ["-s", "https://x"], ["-shttps://x"], ["--cluster", "x"], ["--user", "x"], ["--token", "t"], ["--token=t"], ["--as", "admin"], ["--as-group", "system:masters"], ["--kubeconfig", "/tmp/k"], ["--kubeconfig=/tmp/k"], ["--insecure-skip-tls-verify"], ["--certificate-authority=/x"], ["--client-key=/x"], ["--tls-server-name=x"], ["--username=u"], ["--password=p"]];
    for (const f of kube) expect([f.join(" "), stepFenceIssue(k("-n", "96bulls", ...f, "get", "pods"), PROOF_FENCE)]).toEqual([f.join(" "), "retarget_flag_refused"]);
    for (const f of [["--kubeconfig", "/x"], ["--kube-apiserver=https://x"], ["--kube-token", "t"], ["--kube-as-user", "u"], ["--kube-ca-file=/x"], ["--kube-insecure-skip-tls-verify"]])
      expect(stepFenceIssue(["helm", "--kube-context", ARN, "-n", "96bulls", ...f, "list", "-o", "json"], PROOF_FENCE)).toBe("retarget_flag_refused");
    for (const f of [["--profile", "prod-admin"], ["--endpoint-url=https://x"], ["--no-verify-ssl"], ["--ca-bundle", "/x"]])
      expect(stepFenceIssue(["aws", "eks", "list-clusters", "--region", "us-west-2", ...f], PROOF_FENCE)).toBe(f[0] === "--profile" ? "production_shaped_target" : "retarget_flag_refused");
    expect(stepFenceIssue(["aws", "eks", "list-clusters", "--region", "us-west-2", "--profile", "other"], PROOF_FENCE)).toBe("retarget_flag_refused");
  });
  test("unknown and combined short flags are refused; -A in any form", () => {
    expect(stepFenceIssue(k("-n", "96bulls", "--foo", "get", "pods"), PROOF_FENCE)).toBe("flag_not_allowlisted");
    expect(stepFenceIssue(k("-n", "96bulls", "-it", "get", "pods"), PROOF_FENCE)).toBe("flag_not_allowlisted");
    expect(stepFenceIssue(k("-A", "get", "pods"), PROOF_FENCE)).toBe("all_namespaces_refused");
    expect(stepFenceIssue(k("--all-namespaces=true", "get", "pods"), PROOF_FENCE)).toBe("all_namespaces_refused");
  });
});

describe("(2) identity is bound to live AWS, not to kubeconfig naming", () => {
  test("describe-cluster must be the proof cluster, ACTIVE, an EKS endpoint, with a CA", () => {
    expect("arn" in parseDescribeCluster(describe_())).toBe(true);
    expect(parseDescribeCluster(describe_({ arn: PROOF_CLUSTER_ARN.replace("us-west-2", "eu-west-1") }))).toMatchObject({ error: expect.stringMatching(/is not/) });
    expect(parseDescribeCluster(describe_({ status: "CREATING" }))).toMatchObject({ error: expect.stringMatching(/not ACTIVE/) });
    expect(parseDescribeCluster(describe_({ endpoint: "https://attacker.example" }))).toMatchObject({ error: expect.stringMatching(/not an EKS endpoint/) });
    expect(parseDescribeCluster(describe_({ certificateAuthority: {} }))).toMatchObject({ error: expect.stringMatching(/certificate authority/) });
  });
  test("the kube context's server and CA must equal the described endpoint and CA; TLS overrides fail closed", () => {
    const d = parseDescribeCluster(describe_()) as Exclude<ReturnType<typeof parseDescribeCluster>, { error: string }>;
    expect(bindKubeconfig(view(), ARN, d)).toEqual({ ok: true });
    expect(bindKubeconfig(view({ server: "https://attacker.example", "certificate-authority-data": CA }), ARN, d)).toMatchObject({ error: expect.stringMatching(/server does not equal/) });
    expect(bindKubeconfig(view({ server: ENDPOINT, "certificate-authority-data": Buffer.from("other").toString("base64") }), ARN, d)).toMatchObject({ error: expect.stringMatching(/certificate-authority-data does not equal/) });
    expect(bindKubeconfig(view({ server: ENDPOINT, "certificate-authority-data": CA, "insecure-skip-tls-verify": true }), ARN, d)).toMatchObject({ error: expect.stringMatching(/overrides TLS/) });
    expect(bindKubeconfig(view(undefined, "other"), ARN, d)).toMatchObject({ error: expect.stringMatching(/is missing/) });
  });
  test("observeIdentity: the cluster ARN comes from AWS; a mismatched kubeconfig leaves the run unbound and the guard refuses", () => {
    const calls: string[][] = [];
    const run = (over: { view?: string; account?: string } = {}) => (argv: string[]) => {
      calls.push(argv);
      if (argv[1] === "sts") return { stdout: over.account ?? PROOF_ACCOUNT_ID, status: 0 };
      if (argv[1] === "eks") return { stdout: describe_(), status: 0 };
      return { stdout: over.view ?? view(), status: 0 };
    };
    const ok = observeIdentity(defaultConfig(), run());
    expect(ok.identity).toMatchObject({ clusterBound: true, kubeContextCluster: PROOF_CLUSTER_ARN });
    expect(() => assertProofTarget({ ...ok.identity, confirmLive: true })).not.toThrow();
    const bad = observeIdentity(defaultConfig(), run({ view: view({ server: "https://attacker.example", "certificate-authority-data": CA }) }));
    expect(bad.identity.clusterBound).toBe(false);
    expect(() => assertProofTarget({ ...bad.identity, confirmLive: true })).toThrow(GuardError);
    try {
      assertProofTarget({ ...bad.identity, confirmLive: true });
    } catch (e) {
      expect((e as GuardError).code).toBe("cluster_unbound");
    }
    calls.length = 0;
    observeIdentity(defaultConfig(), run({ account: "111111111111" }));
    expect(calls.map((c) => c[1])).toEqual(["sts"]);
  });
  test("every step uses a generated kubeconfig; the operator's is never consulted", () => {
    const d = parseDescribeCluster(describe_()) as Exclude<ReturnType<typeof parseDescribeCluster>, { error: string }>;
    const kc = JSON.parse(proofKubeconfig(d, ARN));
    expect(kc.clusters).toEqual([{ name: PROOF_CLUSTER_ARN, cluster: { server: ENDPOINT, "certificate-authority-data": CA } }]);
    expect(kc.contexts).toEqual([{ name: ARN, context: { cluster: PROOF_CLUSTER_ARN, user: PROOF_CLUSTER_ARN } }]);
    expect(kc.users[0].user.exec.args).toEqual(["eks", "get-token", "--cluster-name", PROOF_CLUSTER, "--region", "us-west-2", "--output", "json"]);
    // Credentials come only from the exec plugin: no static token, password or client key.
    expect(Object.keys(kc.users[0].user)).toEqual(["exec"]);
    const env = toolEnv({ KUBECONFIG: "/home/op/.kube/config", HELM_KUBECONTEXT: "x", HELM_KUBEAPISERVER: "x", AWS_ENDPOINT_URL: "x", PATH: "/bin" }, "/run/kubeconfig.json");
    expect(env).toMatchObject({ PATH: "/bin", KUBECONFIG: "/run/kubeconfig.json", AWS_CONFIG_FILE: "/dev/null" });
    expect(env.HELM_KUBECONTEXT ?? env.HELM_KUBEAPISERVER ?? env.AWS_ENDPOINT_URL).toBeUndefined();
    const seen: Array<Record<string, string>> = [];
    const spawn: Spawn = (_a, _s, _t, _b, e) => (seen.push(e), { stdout: "", status: 0 });
    const identity = { confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, kubeContextCluster: ARN, clusterBound: true };
    const unbound = new Runner({ mode: "live", identity, fence: PROOF_FENCE, spawn, print: () => {} });
    expect(unbound.run({ id: "x", argv: k("-n", "96bulls", "get", "pods"), mutating: false, touches: [], approval: "operator" }).record.code).toBe("kubeconfig_unbound");
    expect(seen).toHaveLength(0);
    const bound = new Runner({ mode: "live", identity, fence: PROOF_FENCE, spawn, print: () => {}, kubeconfigPath: "/run/kubeconfig.json", baseEnv: { KUBECONFIG: "/home/op/.kube/config", PATH: "/bin" } });
    expect(bound.run({ id: "x", argv: k("-n", "96bulls", "get", "pods"), mutating: false, touches: [], approval: "operator" }).record.status).toBe("ran");
    expect(seen[0]!.KUBECONFIG).toBe("/run/kubeconfig.json");
  });
});

describe("(a) report provenance", () => {
  const T = "2026-10-31T03:00:00.000Z";
  const ran = [{ id: "s", command: "x", mutating: false, touches: [], approval: "operator" as const, status: "ran" as const }];
  const all = CHECK_IDS.map((id): CheckResult => ({ check: id, status: id === "v1-search-scope" ? "unsupported" : "pass", startedAt: T, finishedAt: T, summary: "ok", metrics: {}, failures: [], evidence: [], steps: ran }));
  const CH = "ClickHouse Cloud x (proof.transcripts; feed: harness-synthetic)";
  test("a PASS needs a clean harness commit, a bound stage plan and chart digests", () => {
    expect(finalVerdict(all, CH, PROV)).toEqual({ verdict: "pass", gaps: [] });
    expect(finalVerdict(all, CH, { ...PROV, harness: { ...PROV.harness, dirty: true } })).toEqual({ verdict: "incomplete", gaps: ["harness working tree is dirty"] });
    expect(finalVerdict(all, CH, { ...PROV, stagePlan: null }).gaps).toEqual(["no stage plan bound"]);
    expect(finalVerdict(all, CH, { ...PROV, charts: [{ name: "96bulls", version: "0.3.0", digest: null }] }).gaps).toEqual(["chart package digests missing"]);
    expect(provenanceGaps({ ...PROV, harness: { commit: "unknown", dirty: false } })).toEqual(["harness commit unknown"]);
  });
  test("reports carry provenance and validate it", () => {
    const r = buildReport({ provenance: PROV, runId: "aws-proof-x", generatedAt: "2026-10-31T03:00:01.000Z", mode: "live", target: { stage: "aws-proof", clusterName: PROOF_CLUSTER, kubeContext: ARN, accountId: PROOF_ACCOUNT_ID, region: "us-west-2" }, backends: { metadata: "m", transcript: CH }, checks: all });
    expect(r.verdict).toBe("pass");
    expect(validateReport(r)).toEqual([]);
    expect(validateReport({ ...r, provenance: undefined })).toContain("provenance incomplete (harness commit/dirty, inputs and config sha256, charts)");
  });
  test("git provenance: commit and dirty bit", () => {
    const g = (head: string, status: string) => gitProvenance((argv) => (argv[1] === "rev-parse" ? { stdout: head, status: 0 } : { stdout: status, status: 0 }));
    expect(g("a".repeat(40) + "\n", "")).toEqual({ commit: "a".repeat(40), dirty: false });
    expect(g("a".repeat(40), " M src/x.ts\n")).toEqual({ commit: "a".repeat(40), dirty: true });
    expect(g("fatal", "").commit).toBe("unknown");
  });
  test("chart package digests come from the chart repository index", () => {
    const index = ["apiVersion: v1", "entries:", "  96bulls:", "  - apiVersion: v2", `    digest: ${"1".repeat(64)}`, "    name: 96bulls", "    version: 0.3.0", "  - apiVersion: v2", `    digest: ${"2".repeat(64)}`, "    version: 0.2.0", "  relaycast:", "  - apiVersion: v2", `    digest: ${"3".repeat(64)}`, "    version: 0.1.0", "generated: x"].join("\n");
    expect(chartDigests(index, [{ name: "96bulls", version: "0.3.0" }, { name: "relaycast", version: "0.1.0" }, { name: "relayauth", version: "0.1.0" }])).toEqual([
      { name: "96bulls", version: "0.3.0", digest: "1".repeat(64) },
      { name: "relaycast", version: "0.1.0", digest: "3".repeat(64) },
      { name: "relayauth", version: "0.1.0", digest: null },
    ]);
  });
});

describe("(b) the probe pod runs only allowlisted images", () => {
  const run = (image: string) => k("-n", "relaycast", "run", "proof-relaycast-aws-proof", `--image=${image}`, "--restart=Never", `--labels=${probePod("relaycast", image).labels}`, "--env=RC_URL=http://relaycast.relaycast.svc:8787", "--overrides", probePod("relaycast", image).overrides, "--command", "--", "sleep", "7200");
  test("any other digest-pinned image, from any registry, is refused", () => {
    expect(stepFenceIssue(run(PROOF_PROBE_IMAGES[0]!), PROOF_FENCE)).toBeUndefined();
    expect(stepFenceIssue(run(`docker.io/attacker/tool@sha256:${"a".repeat(64)}`), PROOF_FENCE)).toBe("probe_image_not_allowlisted");
    expect(stepFenceIssue(run(`500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services@sha256:${"b".repeat(64)}`), PROOF_FENCE)).toBe("probe_image_not_allowlisted");
    expect(stepFenceIssue(run("ghcr.io/x:latest"), PROOF_FENCE)).toBe("probe_image_not_digest_pinned");
  });
  test("config refuses a probeImage outside the allowlist", () => {
    expect(validateConfig({ ...defaultConfig(), probeImage: `ghcr.io/x@sha256:${"a".repeat(64)}` }).some((i) => i.startsWith("probeImage must be one of the allowlisted images"))).toBe(true);
    expect(validateConfig({ ...defaultConfig(), probeImage: PROOF_PROBE_IMAGES[0]! })).toEqual([]);
  });
});

describe("(c) the harness binds to the captured Stage A create-only plan", () => {
  const planText = "Plan · aws-proof\nComputing plan\nPlan ready (4.2s)\nPlan: 2 to create\n[cluster] create\n[locker] create\n";
  const inputs = JSON.stringify({ accountId: PROOF_ACCOUNT_ID, region: "us-west-2", eks: { chartVersion: "0.3.0", imageDigest: `sha256:${"5".repeat(64)}`, relayhistoryOrigin: "https://proof-sessions.agentrelay.com", ingress: { hostname: "proof-sessions.agentrelay.com" }, relayauth: { chartVersion: "0.1.0" }, relaycast: { chartVersion: "0.1.0", imageDigest: `sha256:${"6".repeat(64)}` }, sessionDirectory: { enabled: false } } });
  const prov = (over: Record<string, unknown> = {}) => JSON.stringify({ stage: "aws-proof", generatedAt: "2026-10-31T01:32:53.897Z", target: { accountId: PROOF_ACCOUNT_ID, region: "us-west-2" }, source: { commit: "e".repeat(40), clean: true }, inputs: { sanitizedPath: STAGE_PLAN_FILES.inputs, sanitizedSha256: sha256Hex(inputs) }, outputs: { plan: { path: STAGE_PLAN_FILES.plan, sha256: sha256Hex(planText) } }, ...over });
  const files = (o: Partial<Record<keyof typeof STAGE_PLAN_FILES, string>> = {}) => (p: string) =>
    p.endsWith(STAGE_PLAN_FILES.provenance) ? (o.provenance ?? prov()) : p.endsWith(STAGE_PLAN_FILES.plan) ? (o.plan ?? planText) : p.endsWith(STAGE_PLAN_FILES.inputs) ? (o.inputs ?? inputs) : undefined;
  test("a verified plan yields its digests, charts and session-directory state", () => {
    expect(loadStagePlan("/r", files())).toMatchObject({ servicesDigest: `sha256:${"5".repeat(64)}`, relaycastDigest: `sha256:${"6".repeat(64)}`, chartVersions: { "96bulls": "0.3.0", relayauth: "0.1.0", relaycast: "0.1.0" }, sessionDirectoryEnabled: false });
  });
  test("missing, tampered, wrong-stage, unclean or wrong-host plans are refused", () => {
    expect(loadStagePlan("/r", () => undefined)).toMatchObject({ error: expect.stringMatching(/missing/) });
    expect(loadStagePlan("/r", files({ plan: planText + "+ extra\n" }))).toEqual({ error: "stage plan text does not match its recorded sha256" });
    expect(loadStagePlan("/r", files({ provenance: prov({ stage: "aws-sessions-dev" }) }))).toMatchObject({ error: expect.stringMatching(/not aws-proof/) });
    expect(loadStagePlan("/r", files({ provenance: prov({ source: { commit: "e".repeat(40), clean: false } }) }))).toEqual({ error: "stage plan was not captured from a clean commit" });
    const other = inputs.replaceAll("proof-sessions.agentrelay.com", "sessions.agentrelay.com");
    expect(loadStagePlan("/r", files({ inputs: other, provenance: prov({ inputs: { sanitizedPath: STAGE_PLAN_FILES.inputs, sanitizedSha256: sha256Hex(other) } }) }))).toEqual({ error: "stage plan inputs name another host or origin" });
  });
  test("the live cluster must postdate the plan and run its image digests", () => {
    const plan = loadStagePlan("/r", files()) as Exclude<ReturnType<typeof loadStagePlan>, { error: string }>;
    const d = parseDescribeCluster(describe_()) as Exclude<ReturnType<typeof parseDescribeCluster>, { error: string }>;
    expect(bindStagePlan(plan, d)).toEqual({ ok: true });
    expect(bindStagePlan(plan, { ...d, createdAtMs: Date.parse("2026-10-01T00:00:00Z") })).toMatchObject({ error: expect.stringMatching(/predates/) });
    expect(planImageIssues(plan, [plan.servicesDigest, plan.relaycastDigest])).toEqual([]);
    expect(planImageIssues(plan, [`sha256:${"9".repeat(64)}`])).toHaveLength(2);
  });
  test("the CLI refuses a live run without a bound plan, before any AWS call", async () => {
    const errs: string[] = [];
    const orig = console.error;
    console.error = (m: string) => void errs.push(m);
    try {
      expect(await main(["fresh-install", "--confirm-live", "--read-only", "--inputs", "/nonexistent/inputs.json"], {})).toBe(2);
    } finally {
      console.error = orig;
    }
    expect(errs.join("\n")).toMatch(/refusing: stage plan provenance .* is missing/);
  });
});

describe("(d) V1 search gate evidence must come from the authenticated tenant-JWT path", () => {
  const identity = { confirmLive: true, accountId: PROOF_ACCOUNT_ID, clusterName: PROOF_CLUSTER, kubeContext: ARN, kubeContextCluster: ARN, clusterBound: true };
  const spawn: Spawn = (argv, stdin) => {
    if (argv.some((a) => a.includes("currentPrimary"))) return { stdout: "hoopdb-1", status: 0 };
    const tags = [...(stdin ?? "").matchAll(/'tag', '([A-Za-z0-9_]+)'/g)].map((m) => m[1]);
    return { stdout: tags.map((t) => JSON.stringify({ tag: t, rows: [], ms: 1 })).join("\n"), status: 0 };
  };
  const ctxFor = (mode: "rpc-sql" | "postgrest-jwt") => {
    const cfg = { ...defaultConfig(), sessions: 4, search: { mode } };
    const ctx = newCtx(cfg, new Runner({ mode: "live", identity, fence: PROOF_FENCE, spawn, print: () => {}, kubeconfigPath: "/run/k.json" }), "aws-proof-search-0001", {});
    ctx.print = () => {};
    ctx.searchable = new Set(ctx.corpus.map((p) => p.sessionId));
    return ctx;
  };
  test("the psql SET ROLE path is a diagnostic: a clean result is BLOCKED, never UNSUPPORTED/PASS", async () => {
    const r = await v1SearchScope(ctxFor("rpc-sql"));
    expect(r.status).toBe("blocked");
    expect(r.summary).toMatch(/^diagnostic only \(psql SET ROLE\), not gate evidence/);
  });
  test("the authenticated HTTP path counts as gate evidence", async () => {
    const ctx = ctxFor("postgrest-jwt");
    ctx.httpSearch = async () => ({ rows: [], ms: 2 });
    expect((await v1SearchScope(ctx)).status).toBe("unsupported");
  });
  test("the exchange and PostgREST requests have the customer's shape", async () => {
    const seen: Array<{ url: string; init?: RequestInit }> = [];
    const f = async (url: string, init?: RequestInit) => {
      seen.push({ url, init });
      return url.endsWith("/v1/tokens/session-directory-search")
        ? new Response(JSON.stringify({ accessToken: "search-token-xyz", tokenType: "Bearer" }), { status: 201 })
        : new Response(JSON.stringify([{ session_id: "s1", org_id: "org_proof_a", short_summary: "x" }]), { status: 200 });
    };
    const token = await exchangeSearchToken({ relayauthUrl: "http://127.0.0.1:1", exchangeKey: "rak_k", subjectToken: "subject-jwt", workspaceId: "default", fetch: f });
    expect(token).toBe("search-token-xyz");
    expect(new Headers(seen[0]!.init?.headers).get("x-api-key")).toBe("rak_k");
    expect(JSON.parse(String(seen[0]!.init?.body))).toEqual({ subjectToken: "subject-jwt", workspaceId: "default" });
    const res = await postgrestSearch({ postgrestUrl: "http://127.0.0.1:2", token, workspaceId: "default", q: "zqterm", fetch: f });
    expect(res.rows).toEqual([{ session_id: "s1", org_id: "org_proof_a", short_summary: "x" }]);
    const h = new Headers(seen[1]!.init?.headers);
    expect([seen[1]!.url, h.get("authorization"), h.get("content-profile")]).toEqual(["http://127.0.0.1:2/rpc/search_session_directory", "Bearer search-token-xyz", "sessions"]);
    expect(JSON.parse(String(seen[1]!.init?.body))).toEqual({ p_workspace_id: "default", p_query: "zqterm" });
    await expect(exchangeSearchToken({ relayauthUrl: "x", exchangeKey: "k", subjectToken: "s", workspaceId: "w", fetch: async () => new Response(JSON.stringify({ code: "subject_token_invalid" }), { status: 401 }) })).rejects.toThrow(/subject_token_invalid/);
  });
});
