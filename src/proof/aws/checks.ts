/**
 * The nine proof checks. Each returns one CheckResult. In dry-run every check
 * prints its exact steps and returns `planned`; in read-only, mutating steps are
 * skipped and the check is `skipped` or `blocked`; live runs everything the
 * guard allows. See docs/aws-e2e-proof.md for the approval boundary.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { gunzipSync } from "node:zlib";
import type { ProofConfig } from "./config.ts";
import { allCanaries, expectedShortSummary, FAKE_AWS_KEY_ID, plantCorpus, telemetryForbidden, type PlantedSession } from "./corpus.ts";
import { evaluatePlantedTrace, evaluateStaleSummary, evaluateV1Scope, type SessionObservation, type StaleObservation } from "./evaluate.ts";
import type { Runner, Step } from "./exec.ts";
import { evaluateFreshInstall, expectedFromValues, parseChartRef, type HelmRelease, type PodList } from "./fresh-install.ts";
import { IngestError, locationOf, mintTenantToken, newUploadState, plantNextRevision, type Fetch, type UploadState } from "./ingest.ts";
import { crossTenantHits, refusedAsExpected, scanForCanaries, tenantKey, type ProbeObservation, type Tenant } from "./leak-scan.ts";
import {
  backupRestoreSteps, cnpgInstancesStep, failoverSteps, failoverVerdict, helmHistoryStep, relaycastProbeSteps, relaycastRestartSteps,
  relaycastVerdict, restoreCleanupSteps, RESTORE_CLUSTER, rolloutRestartSteps, evaluateBackup, STAGE_DESTROY_COMMAND, teardownResidue, teardownSteps, clusterGone, PROOF_TAG,
  teardownVerifySteps, upgradeRollbackSteps, versioningEnabled,
} from "./lifecycle.ts";
import { directorySql, indexStateSql, jsonLines, kubectl, lastJson, primaryStep, s3ListStep, searchBatchSql, sqlStep, stageTimesSql, type SearchResultLine, type SearchRow } from "./probes.ts";
import { evidence, TRANSCRIPT_GATE_DEFERRAL, type CheckId, type CheckResult, type Evidence, type Status } from "./report.ts";
import { summarize } from "./stats.ts";
import { manifestKey, scrubText } from "../../shared/vendor.ts";
import { PROOF_BACKUP_BUCKET, PROOF_POSTGREST, PROOF_RELAYAUTH_TLS, PROOF_SEARCH_CLIENT_POD, PROOF_UNLISTED_ORG } from "./fence.ts";
import { SEARCH_EXCHANGE_KEY_ENV } from "./search-http.ts";
import { probePod } from "./ops.ts";
import { planImageIssues, type StagePlan } from "./provenance.ts";
import { backendLabel, clickhouseBackend, credentialsFromEnv, evaluateTranscriptGate, type TranscriptBackend, type TranscriptObservation } from "./transcript.ts";
import {
  burstPlan, evaluateBurst, evaluateListeners, evaluateRoutes, evaluateTls, evaluateWafConfig, hostIsProofAlb, ingestRoutes, probeTls, rateRules, resolveAll,
  runBurst, tcpPort, type AlbListener, type Credential, type RouteObservation, type RouteProbe, type SslPolicy, type WebAcl,
} from "./ingress.ts";

export interface Ctx {
  cfg: ProofConfig;
  runner: Runner;
  runId: string;
  print: (line: string) => void;
  /** Run-scoped secrets (minted tokens) the sanitizer must strip. */
  secrets: string[];
  corpus: PlantedSession[];
  uploads: Map<string, UploadState>;
  /** One token per tenant (its first machine's), for reads and probes. */
  tokens: Map<string, string>;
  /** One RelayAuth assertion per planted machine, used only for that machine's writes. */
  machineTokens: Map<string, string>;
  /** Set by planted-trace: sessions confirmed searchable, the baseline later checks compare to. */
  searchable?: Set<string>;
  sleep: (ms: number) => Promise<void>;
  now: () => number;
  env: Record<string, string | undefined>;
  /** Override for tests; otherwise built from cfg.transcriptSearch and the environment. */
  transcriptBackend?: TranscriptBackend;
  /** The captured Stage A plan the live cluster was bound to (provenance.ts). */
  stagePlan?: StagePlan;
  /** Searches served by the psql diagnostic path. Checks that used it cannot pass. */
  diagnosticSearches: number;
  /** The in-cluster search client pod is up. */
  searchClientReady?: boolean;
  /** Override for tests: the authenticated HTTP search (exchange + PostgREST). */
  httpSearch?: (t: Tenant, q: string) => Promise<{ rows: SearchRow[]; ms: number }>;
}

export function newCtx(cfg: ProofConfig, runner: Runner, runId: string, env: Record<string, string | undefined> = process.env): Ctx {
  return {
    cfg, runner, runId, env,
    print: (l) => console.log(l),
    secrets: [],
    corpus: plantCorpus({ runId, sessions: cfg.sessions, tenants: cfg.tenants, ...(cfg.objectKeyPrefixBase ? { keyPrefixBase: cfg.objectKeyPrefixBase } : {}) }),
    uploads: new Map(),
    tokens: new Map(),
    machineTokens: new Map(),
    diagnosticSearches: 0,
    sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
    now: () => Date.now(),
  };
}

const iso = (ms: number) => new Date(ms).toISOString();

class Result {
  readonly startedAt: number;
  readonly failures: string[] = [];
  readonly evidence: Evidence[] = [];
  readonly metrics: Record<string, unknown> = {};
  private readonly firstStep: number;
  private readonly firstDiagnostic: number;
  constructor(private readonly ctx: Ctx, readonly check: CheckId) {
    this.startedAt = ctx.now();
    this.firstStep = ctx.runner.records.length;
    this.firstDiagnostic = ctx.diagnosticSearches;
    ctx.print(`\n== ${check} (${ctx.runner.mode})`);
  }
  ev(label: string, raw: string, withExcerpt = true) {
    this.evidence.push(evidence(label, raw, { secrets: this.ctx.secrets, canaries: allCanaries(this.ctx.corpus) }, withExcerpt));
  }
  done(status: Status, summaryIn: string): CheckResult {
    let summary = summaryIn;
    const steps = this.ctx.runner.records.slice(this.firstStep);
    // A pass with recorded failures is a fail; a fail needs at least one stated reason.
    let final: Status = status === "pass" && this.failures.length ? "fail" : status;
    // Search evidence from the psql diagnostic path is never gate evidence.
    if ((final === "pass" || final === "unsupported") && this.ctx.diagnosticSearches > this.firstDiagnostic) {
      final = "blocked";
      summary = `diagnostic only (psql SET ROLE), not gate evidence; the authenticated tenant-JWT search path must be proven. ${summary}`;
    }
    if (final === "fail" && !this.failures.length) this.failures.push(summary);
    this.ctx.print(`   -> ${final.toUpperCase()}: ${summary}`);
    return { check: this.check, status: final, startedAt: iso(this.startedAt), finishedAt: iso(Math.max(this.ctx.now(), this.startedAt)), summary, metrics: this.metrics, failures: this.failures, evidence: this.evidence, steps };
  }
}

const dry = (ctx: Ctx) => ctx.runner.mode === "dry-run";

/** Mode-appropriate terminal status when the check could not reach a verdict. */
function notRun(ctx: Ctx, r: Result, summary: string): CheckResult {
  if (dry(ctx)) return r.done("planned", summary);
  if (ctx.runner.mode === "read-only") return r.done("skipped", `${summary} (needs mutating steps; read-only mode)`);
  return r.done("blocked", summary);
}

/* ================================================================ 1. fresh install */

export async function freshInstall(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "fresh-install");
  const c = ctx.cfg;
  const nss = [...new Set([c.namespace, c.relayauth.namespace, c.relaycast.namespace, "cnpg-system"])];
  const pods = nss.map((ns) => ctx.runner.run({ id: `pods.${ns}`, argv: kubectl(c, "-n", ns, "get", "pods", "-o", "json"), mutating: false, touches: [`k8s:${ns}/pods (list)`], approval: "operator" }));
  // Per namespace, never -A.
  const helmLists = nss.map((ns) => ctx.runner.run({ id: `helm.list.${ns}`, argv: ["helm", "--kube-context", c.kubeContext, "-n", ns, "list", "-o", "json"], mutating: false, touches: [`helm:${ns} releases (list)`], approval: "operator" }));
  const helm = { ok: helmLists.every((h) => h.ok), stdout: JSON.stringify(helmLists.flatMap((h) => (h.ok ? (JSON.parse(h.stdout || "[]") as unknown[]) : []))), record: helmLists.find((h) => !h.ok)?.record ?? helmLists[0]!.record };
  const showValues = (name: string, version: string) => ctx.runner.run({ id: `helm.show-values.${name}-${version}`, argv: ["helm", "show", "values", name, "--repo", c.chartRepo, "--version", version], mutating: false, touches: [`chart repo ${c.chartRepo} (read)`], approval: "operator" });
  if (dry(ctx)) {
    showValues("96bulls", c.chartVersion);
    return r.done("planned", "lists running pods and helm releases; compares every image digest with each deployed chart's released values and the operator's inputs");
  }
  const merged: PodList = { items: [] };
  for (const p of pods) {
    if (!p.ok) {
      r.failures.push(`pod listing failed: ${p.record.code}`);
      continue;
    }
    merged.items.push(...(JSON.parse(p.stdout) as PodList).items);
  }
  const releases = helm.ok ? (JSON.parse(helm.stdout) as HelmRelease[]) : [];
  if (!helm.ok) r.failures.push(`helm list failed: ${helm.record.code}`);
  // Released defaults of every deployed chart, then the operator's inputs (per-account ECR digest) on top.
  const charts = [...new Set(releases.map((x) => x.chart))].map(parseChartRef).filter((x): x is { name: string; version: string } => !!x);
  const released: Record<string, string> = {};
  const unavailable: string[] = [];
  for (const ch of charts) {
    const v = showValues(ch.name, ch.version);
    if (v.ok) Object.assign(released, expectedFromValues(v.stdout));
    else unavailable.push(`${ch.name}-${ch.version}`);
  }
  if (unavailable.length) r.metrics.chartValuesUnavailable = unavailable;
  // The bound stage plan's digests win over chart defaults and operator inputs.
  const plan = ctx.stagePlan;
  const expected = {
    ...released, ...c.expectedImages,
    ...(plan ? { "500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services": plan.servicesDigest, "ghcr.io/agentworkforce/relaycast-self-host": plan.relaycastDigest } : {}),
  };
  const v = evaluateFreshInstall(merged, releases, expected);
  r.failures.push(...v.failures);
  if (plan) r.failures.push(...planImageIssues(plan, v.images.map((i) => i.specDigest ?? "")));
  else if (c.stage === "aws-proof") r.failures.push("no stage plan bound; the running images cannot be matched to the captured plan");
  r.metrics.charts = v.charts;
  r.metrics.images = v.images.map(({ namespace, pod, container, image, specDigest, runningDigest }) => ({ namespace, pod, container, image, specDigest: specDigest ?? null, runningDigest: runningDigest ?? null }));
  r.metrics.expectedRepositories = Object.keys(expected).length;
  r.ev("pods.json", JSON.stringify(merged), false);
  r.ev("helm-list.json", helm.stdout);
  if (v.blocked) return r.done("blocked", "no released chart values or input digests to compare against");
  return r.done(v.ok ? "pass" : "fail", v.ok ? `${v.images.length} running images digest-pinned and match the release` : `${r.failures.length} image/release problem(s)`);
}

/* ================================================================ public ingress */

const awsRead = (c: ProofConfig, id: string, args: string[], touches: string): Step => ({ id, argv: ["aws", ...args, "--region", c.region, "--output", "json"], mutating: false, touches: [touches], approval: "operator" });

export async function publicIngress(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "public-ingress");
  const c = ctx.cfg;
  const host = c.ingress.hostname;
  r.metrics.host = host;
  const ing = ctx.runner.run({ id: "ingress.k8s", argv: kubectl(c, "-n", c.namespace, "get", "ingress", "-o", "json"), mutating: false, touches: [`k8s:${c.namespace}/ingress (read)`], approval: "operator" });
  const lbs = ctx.runner.run(awsRead(c, "ingress.alb.list", ["elbv2", "describe-load-balancers"], `aws:elbv2 load balancers in ${c.accountId} (list)`));
  if (dry(ctx)) {
    for (const s of [
      awsRead(c, "ingress.alb.listeners", ["elbv2", "describe-listeners", "--load-balancer-arn", "<proof ALB arn>"], "aws:elbv2 listeners (read)"),
      awsRead(c, "ingress.alb.ssl-policy", ["elbv2", "describe-ssl-policies", "--names", "<listener SslPolicy>"], "aws:elbv2 ssl policy (read)"),
      awsRead(c, "ingress.waf", ["wafv2", "get-web-acl-for-resource", "--resource-arn", "<proof ALB arn>"], "aws:wafv2 web ACL association (read)"),
    ]) ctx.runner.run(s);
    for (const [id, desc, mutating] of [
      ["ingress.dns-guard", `DNS ${host} must resolve to the proof ALB's addresses, else every active probe is refused`, false],
      ["ingress.healthz", `GET https://${host}/healthz expects 200`, false],
      ["ingress.negative.none", `9 ingest routes without credentials expect 401/403`, false],
      ["ingress.negative.wrong-tenant", `8 ingest routes with tenant A's token against tenant B's objects expect 401/403 (404 on by-id)`, false],
      ["ingress.port80", `TCP ${host}:80 expects no listener`, false],
      ["ingress.tls", `TLS 1.0/1.1 handshake must fail; TLS 1.2+ must verify the certificate for ${host}`, false],
      ["ingress.waf.burst", `bounded GET burst to https://${host}${c.ingress.burst.path ?? "/healthz"} until the first WAF block (cap above the rule limit, <= 5000, <= 300 s)`, true],
    ] as const) ctx.runner.record({ id, argv: ["PROBE", desc], mutating, touches: [`public:${host}`], approval: mutating ? "khaliq" : "operator" });
    return r.done("planned", `public ingress ${host}: /healthz 200, refusals without/with wrong credentials, no :80, TLS >= 1.2 + valid cert, WAF rate rule blocks a bounded burst`);
  }
  // Find the ALB that serves the proof Ingress, in the proof account.
  const albDns = ((JSON.parse(ing.stdout || "{}") as { items?: Array<{ status?: { loadBalancer?: { ingress?: Array<{ hostname?: string }> } } }> }).items ?? [])
    .flatMap((i) => i.status?.loadBalancer?.ingress ?? []).map((x) => x.hostname).find(Boolean);
  const alb = ((JSON.parse(lbs.stdout || "{}") as { LoadBalancers?: Array<{ LoadBalancerArn: string; DNSName: string }> }).LoadBalancers ?? [])
    .find((l) => albDns && l.DNSName.toLowerCase().replace(/^dualstack\./, "") === albDns.toLowerCase().replace(/^dualstack\./, ""));
  if (!alb) return r.done("blocked", "could not find the proof Ingress's ALB in the proof account");
  r.metrics.albArn = alb.LoadBalancerArn;
  const listeners = ctx.runner.run(awsRead(c, "ingress.alb.listeners", ["elbv2", "describe-listeners", "--load-balancer-arn", alb.LoadBalancerArn], `aws:${alb.LoadBalancerArn} listeners (read)`));
  const ls = ((JSON.parse(listeners.stdout || "{}") as { Listeners?: AlbListener[] }).Listeners ?? []);
  const policyName = ls.find((l) => l.Protocol === "HTTPS" && l.Port === 443)?.SslPolicy;
  const pol = policyName ? ctx.runner.run(awsRead(c, "ingress.alb.ssl-policy", ["elbv2", "describe-ssl-policies", "--names", policyName], "aws:elbv2 ssl policy (read)")) : undefined;
  const policy = pol?.ok ? (JSON.parse(pol.stdout) as { SslPolicies?: SslPolicy[] }).SslPolicies?.[0] : undefined;
  const waf = ctx.runner.run(awsRead(c, "ingress.waf", ["wafv2", "get-web-acl-for-resource", "--resource-arn", alb.LoadBalancerArn], `aws:wafv2 association for ${alb.LoadBalancerArn} (read)`));
  const acl = waf.ok ? (JSON.parse(waf.stdout || "{}") as { WebACL?: WebAcl }).WebACL : undefined;
  r.metrics.sslPolicy = policy ? { name: policy.Name, protocols: policy.SslProtocols } : null;
  r.metrics.listeners = ls.map((l) => ({ port: l.Port, protocol: l.Protocol, sslPolicy: l.SslPolicy ?? null, acm: !!l.Certificates?.some((x) => /^arn:aws:acm:/.test(x.CertificateArn)) }));
  r.metrics.waf = acl ? { name: acl.Name, rateRules: rateRules(acl) } : null;
  const sub = (k: string, f: string[]) => {
    (r.metrics.subchecks as Record<string, unknown>)[k] = f.length ? { status: "fail", failures: f } : { status: "pass" };
    r.failures.push(...f.map((x) => `${k}: ${x}`));
  };
  r.metrics.subchecks = {};
  sub("d.waf-config", evaluateWafConfig(acl));
  // Active probes only when the host demonstrably reaches this ALB.
  const [hostAddrs, albAddrs] = [await resolveAll(host), await resolveAll(alb.DNSName)];
  const reachesProof = hostIsProofAlb(hostAddrs, albAddrs);
  r.metrics.hostResolvesToProofAlb = reachesProof;
  const g = ctx.runner.gate({ id: "ingress.active-probes", argv: ["PROBE", `https://${host}`], mutating: false, touches: [`public:${host}`], approval: "operator" });
  ctx.runner.note(reachesProof ? g.record : { ...g.record, status: "refused", code: "host_not_proof_alb" });
  if (!g.allowed || !reachesProof) {
    sub("b.listeners", evaluateListeners(ls, "closed").filter((f) => !f.startsWith("TCP")));
    sub("c.tls-policy", evaluateTls(policy, { legacyAccepted: false, modern: { ok: true, subjectAltName: `DNS:${host}` } }, host, ctx.now()).filter((f) => f.startsWith("SSL policy") || f.startsWith("ALB SSL")));
    return r.done(r.failures.length ? "fail" : "blocked", reachesProof ? "active probes not allowed in this mode" : `${host} does not resolve to the proof ALB (proxied or pointed elsewhere); active probes refused, ALB configuration checked only`);
  }
  const base = `https://${host}`;
  const healthz = await fetch(`${base}/healthz`).then((x) => x.status, () => undefined);
  const other = ctx.corpus.find((p) => tenantKey(p.tenant) === tenantKey(c.tenants[1]!))!;
  const routes = ingestRoutes({ otherTraceId: other.traceId, otherManifestKey: manifestKey(locationOf(other)), other: locationOf(other) });
  const obs: RouteObservation[] = [];
  const probe = async (route: RouteProbe, credential: Credential, token?: string) => {
    const headers: Record<string, string> = { ...(token ? { authorization: `Bearer ${token}` } : {}) };
    if (route.method === "POST") headers["content-type"] = route.path.startsWith("/v1/pipeline/chunks") ? "application/octet-stream" : "application/json";
    // The manifest route also checks these against the body; they name the other tenant's session.
    if (route.path === "/v1/pipeline/manifests") Object.assign(headers, { "x-machine-id": other.machineId, "x-session-id": other.sessionId });
    const res = await fetch(base + route.path, { method: route.method, headers, ...(route.method === "POST" ? { body: route.body ?? "" } : {}) }).then((x) => x.status, () => 0);
    obs.push({ route, credential, status: res });
  };
  for (const rt of routes) await probe(rt, "none");
  const tokA = ctx.tokens.get(tenantKey(c.tenants[0]!));
  if (tokA) for (const rt of routes.filter((x) => !x.path.startsWith("/v1/pipeline/events"))) await probe(rt, "wrong-tenant", tokA);
  else r.metrics.wrongTenantProbes = "skipped: no tenant token (run planted-trace in the same invocation)";
  sub("a.negative-probes", evaluateRoutes(obs, healthz));
  r.metrics.routeProbes = obs.map((o) => ({ method: o.route.method, path: o.route.path.split("?")[0], credential: o.credential, status: o.status }));
  sub("b.listeners", evaluateListeners(ls, await tcpPort(host, 80)));
  const tls = await probeTls(host);
  r.metrics.tls = { legacyAccepted: tls.legacyAccepted, protocol: tls.modern.protocol ?? null, validTo: tls.modern.validTo ?? null, note: "a refused legacy handshake may be the local OpenSSL floor; the ALB SSL policy is the authoritative evidence" };
  sub("c.tls", evaluateTls(policy, tls, host, ctx.now()));
  // (d) the bounded burst: mutating in effect (it can rate-limit the operator's IP), Khaliq-approved, live only.
  const plan = burstPlan(acl, c.ingress.burst);
  if ("refused" in plan) sub("d.waf-burst", [plan.refused]);
  else {
    const bg = ctx.runner.gate({ id: "ingress.waf.burst", argv: ["PROBE", `GET ${base}${plan.path}`, `x<=${plan.maxRequests}`, `concurrency=${plan.concurrency}`], mutating: true, touches: [`public:${host} (bounded burst; may rate-limit this operator IP for the rule window)`], approval: "khaliq" });
    ctx.runner.note(bg.record);
    if (bg.allowed) {
      const res = await runBurst(base, plan);
      r.metrics.burst = { ...res, plan };
      sub("d.waf-burst", evaluateBurst(res, plan));
    } else (r.metrics.subchecks as Record<string, unknown>)["d.waf-burst"] = { status: bg.record.status };
  }
  return r.done(r.failures.length ? "fail" : "pass", `${host}: healthz 200, ${obs.length} refusals, no :80, TLS ${tls.modern.protocol ?? "?"}, WAF ${acl?.Name ?? "?"}`);
}

/* ================================================================ shared live plumbing */

interface Forward {
  url: string;
  stop(): void;
}

/** `kubectl port-forward` in the background, only after the guard allowed it. */
async function portForward(ctx: Ctx, ns: string, svc: string, port: number, localPort: number): Promise<Forward | undefined> {
  const argv = kubectl(ctx.cfg, "-n", ns, "port-forward", `svc/${svc}`, `${localPort}:${port}`);
  const g = ctx.runner.gate({ id: `port-forward.${svc}`, argv, mutating: false, touches: [`k8s:${ns}/svc/${svc} (tunnel through the API server)`], approval: "operator" });
  const unbound = g.allowed && ctx.runner.needsKubeconfig(argv) && !ctx.runner.hasKubeconfig;
  ctx.runner.note(unbound ? { ...g.record, status: "refused", code: "kubeconfig_unbound" } : g.record);
  if (!g.allowed || unbound) return undefined;
  const child: ChildProcess = spawn(argv[0]!, argv.slice(1), { stdio: ["ignore", "pipe", "ignore"], env: ctx.runner.env(), shell: false });
  await new Promise<void>((resolve) => {
    const t = setTimeout(resolve, 5_000);
    child.stdout?.on("data", (d: Buffer) => {
      if (d.toString().includes("Forwarding from")) {
        clearTimeout(t);
        resolve();
      }
    });
  });
  return { url: `http://127.0.0.1:${localPort}`, stop: () => child.kill("SIGTERM") };
}

function primary(ctx: Ctx): string {
  const o = ctx.runner.run(primaryStep(ctx.cfg));
  return o.ok ? o.stdout.trim() : "";
}

function sql<T>(ctx: Ctx, id: string, pod: string, text: string): T | undefined {
  const o = ctx.runner.run(sqlStep(ctx.cfg, id, pod, text));
  return o.ok ? lastJson<T>(o.stdout) : undefined;
}

/**
 * Search every query for one tenant. `postgrest-jwt` (gate evidence): the
 * tenant's RelayAuth token is exchanged for a search token and each query goes
 * to PostgREST. `rpc-sql`: one psql session as the Worker role, counted as a
 * diagnostic so the check using it cannot pass.
 */
async function searchTenant(ctx: Ctx, pod: string, t: Tenant, queries: Array<{ tag: string; q: string }>, id: string): Promise<Map<string, SearchResultLine> | undefined> {
  if (queries.length === 0) return new Map();
  if (ctx.cfg.search.mode === "rpc-sql") {
    ctx.diagnosticSearches += 1;
    const o = ctx.runner.run(sqlStep(ctx.cfg, id, pod, searchBatchSql(t, queries)));
    if (!o.ok) return undefined;
    return new Map(jsonLines<SearchResultLine>(o.stdout).filter((l) => typeof l.tag === "string").map((l) => [l.tag, l]));
  }
  if (ctx.cfg.search.mode !== "postgrest-jwt") return undefined;
  if (!ctx.httpSearch) return inClusterSearch(ctx, t, queries, id);
  const search = ctx.httpSearch;
  const out = new Map<string, SearchResultLine>();
  for (const { tag, q } of queries) {
    try {
      const r = await search(t, q);
      out.set(tag, { tag, rows: r.rows, ms: r.ms });
    } catch {
      /* missing line = search error for that query */
    }
  }
  return out;
}

/**
 * The in-cluster search client. NetworkPolicy admits only labelled callers to
 * RelayAuth's TLS Service and to PostgREST, and the exchange must never use the
 * plain relayauth:8787, so the search runs from a pod carrying both caller
 * labels and trusting the relayauth-ca ConfigMap. The exchange key and subject
 * token reach it on stdin over `kubectl exec` (API server TLS), embedded in the
 * script; they are never in argv, env, a file, or the report.
 */
export const SEARCH_CLIENT_SOURCE = String.raw`
const fs = await import("node:fs");
const out = (o) => console.log(JSON.stringify(o));
let ca;
try { ca = fs.readFileSync("/ca/ca.crt", "utf8"); } catch { for (const q of INPUT.queries) out({ tag: q.tag, error: "ca_missing" }); process.exit(0); }
const ex = await fetch("https://${PROOF_RELAYAUTH_TLS.service}.${PROOF_RELAYAUTH_TLS.namespace}.svc:${PROOF_RELAYAUTH_TLS.port}/v1/tokens/session-directory-search", {
  method: "POST", headers: { "x-api-key": INPUT.exchangeKey, "content-type": "application/json" },
  body: JSON.stringify({ subjectToken: INPUT.subjectToken, workspaceId: INPUT.workspaceId }), tls: { ca },
}).catch(() => null);
if (!ex || ex.status !== 201) { for (const q of INPUT.queries) out({ tag: q.tag, error: "exchange_" + (ex ? ex.status : "unreachable") }); process.exit(0); }
const { accessToken } = await ex.json();
for (const q of INPUT.queries) {
  const t0 = performance.now();
  const r = await fetch("http://${PROOF_POSTGREST.service}.${PROOF_POSTGREST.namespace}.svc:${PROOF_POSTGREST.port}/rpc/search_session_directory", {
    method: "POST", headers: { authorization: "Bearer " + accessToken, "content-profile": "sessions", "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ p_workspace_id: INPUT.workspaceId, p_query: q.q }),
  }).catch(() => null);
  const ms = performance.now() - t0;
  if (!r || r.status !== 200) { out({ tag: q.tag, error: "postgrest_" + (r ? r.status : "unreachable"), ms }); continue; }
  const rows = (await r.json()).map((x) => ({ session_id: String(x.session_id), org_id: String(x.org_id), short_summary: typeof x.short_summary === "string" ? x.short_summary : null }));
  out({ tag: q.tag, rows, ms });
}
`;

export function searchClientSteps(c: ProofConfig): Step[] {
  const img = c.probeImage ?? "<allowlisted probe image>";
  const spec = probePod("search", img);
  return [
    { id: "search-client.pod", argv: kubectl(c, "-n", spec.namespace, "run", spec.name, `--image=${img}`, "--restart=Never", `--labels=${spec.labels}`, "--overrides", spec.overrides, "--command", "--", "sleep", "7200"), mutating: true, touches: [`k8s:${spec.namespace}/pod/${spec.name} (created; RelayAuth TLS + PostgREST caller labels)`], approval: "khaliq" },
    { id: "search-client.ready", argv: kubectl(c, "-n", spec.namespace, "wait", "--for=condition=Ready", `pod/${spec.name}`, "--timeout=180s"), mutating: false, touches: [`k8s:${spec.namespace}/pod/${spec.name}`], approval: "operator", timeoutMs: 200_000 },
  ];
}

/** One tenant's queries through the in-cluster client; undefined when the path is unavailable. */
async function inClusterSearch(ctx: Ctx, t: Tenant, queries: Array<{ tag: string; q: string }>, id: string): Promise<Map<string, SearchResultLine> | undefined> {
  const key = ctx.env[SEARCH_EXCHANGE_KEY_ENV];
  const subject = ctx.tokens.get(tenantKey(t));
  if (!key || !subject) return undefined;
  if (!ctx.secrets.includes(key)) ctx.secrets.push(key);
  if (!ctx.searchClientReady) {
    for (const s of searchClientSteps(ctx.cfg)) if (!ctx.runner.run(s).ok) return undefined;
    ctx.searchClientReady = true;
  }
  const input = { exchangeKey: key, subjectToken: subject, workspaceId: t.workspaceId, queries };
  const o = ctx.runner.run({ id, argv: kubectl(ctx.cfg, "-n", PROOF_POSTGREST.namespace, "exec", "-i", PROOF_SEARCH_CLIENT_POD, "--", "bun", "run", "-"), stdin: `const INPUT = ${JSON.stringify(input)};\n${SEARCH_CLIENT_SOURCE}`, mutating: false, touches: [`relayauth-tls (search token exchange)`, `postgrest:${PROOF_POSTGREST.namespace}/${PROOF_POSTGREST.service} (read)`], approval: "operator", timeoutMs: 180_000 });
  if (!o.ok) return undefined;
  const out = new Map<string, SearchResultLine>();
  for (const l of jsonLines<{ tag?: string; rows?: SearchRow[]; ms?: number; error?: string }>(o.stdout))
    if (typeof l.tag === "string" && Array.isArray(l.rows)) out.set(l.tag, { tag: l.tag, rows: l.rows, ms: Number(l.ms ?? 0) });
  return out;
}

const tagOf = (p: PlantedSession, kind: string) => `${kind}_${p.sessionId.replace(/-/g, "")}`;

/** Search each planted session's summaryTerm as its own tenant; returns found + latency per session. */
async function searchOwn(ctx: Ctx, pod: string, sessions: readonly PlantedSession[], label: string) {
  const out = new Map<string, { found: boolean; ms?: number; summaries: Array<string | null>; error?: string }>();
  for (const t of ctx.cfg.tenants) {
    const mine = sessions.filter((p) => tenantKey(p.tenant) === tenantKey(t));
    const res = await searchTenant(ctx, pod, t, mine.map((p) => ({ tag: tagOf(p, "own"), q: p.summaryTerm })), `search.${label}.${t.orgId}`);
    for (const p of mine) {
      const line = res?.get(tagOf(p, "own"));
      if (!line) out.set(p.sessionId, { found: false, summaries: [], error: res ? "no_result_line" : "search_failed" });
      else {
        const rows = line.rows.filter((x) => x.session_id === p.sessionId);
        out.set(p.sessionId, { found: rows.length > 0, ms: line.ms, summaries: rows.map((x) => x.short_summary) });
      }
    }
  }
  return out;
}

const SESSION_CONCURRENCY = 8;

async function inBatches<T>(items: readonly T[], fn: (t: T) => Promise<void>) {
  for (let i = 0; i < items.length; i += SESSION_CONCURRENCY) await Promise.all(items.slice(i, i + SESSION_CONCURRENCY).map(fn));
}

async function withForwards<T>(ctx: Ctx, fn: (ingest: Forward, relayauth: Forward) => Promise<T>): Promise<T | undefined> {
  const ingest = await portForward(ctx, ctx.cfg.namespace, ctx.cfg.ingest.service, ctx.cfg.ingest.port, 18080);
  const ra = await portForward(ctx, ctx.cfg.relayauth.namespace, ctx.cfg.relayauth.service, ctx.cfg.relayauth.port, 18787);
  try {
    if (!ingest || !ra) return undefined;
    return await fn(ingest, ra);
  } finally {
    ingest?.stop();
    ra?.stop();
  }
}

export async function tokensFor(ctx: Ctx, relayauthUrl: string, f?: Fetch): Promise<string | undefined> {
  const bootstrapKey = ctx.env.PROOF_RELAYAUTH_BOOTSTRAP_KEY;
  const sponsorId = ctx.env.PROOF_SPONSOR_ID ?? "proof-operator";
  if (!bootstrapKey) return "PROOF_RELAYAUTH_BOOTSTRAP_KEY is not set (pipe it from 1Password; never a file)";
  if (!ctx.secrets.includes(bootstrapKey)) ctx.secrets.push(bootstrapKey);
  const machines = new Map<string, Tenant>();
  for (const p of ctx.corpus) {
    const existing = machines.get(p.machineId);
    if (existing && tenantKey(existing) !== tenantKey(p.tenant)) return `machine ${p.machineId} is planted for two tenants`;
    machines.set(p.machineId, p.tenant);
  }
  for (const [machineId, t] of machines) {
    if (ctx.machineTokens.has(machineId)) continue;
    const g = ctx.runner.gate({ id: `relayauth.mint.${t.orgId}.${machineId}`, argv: ["HTTP", "POST", `${relayauthUrl}/v1/tokens/relayhistory-assertion`], mutating: true, touches: [`relayauth:${ctx.cfg.relayauth.namespace} (1h token for ${t.orgId}, bound to ${machineId})`], approval: "khaliq" });
    if (!g.allowed) {
      ctx.runner.note(g.record);
      return "token minting not allowed in this mode";
    }
    try {
      const tok = await mintTenantToken({ relayauthUrl, bootstrapKey, orgId: t.orgId, workspaceId: t.workspaceId, machineId, sponsorId, historyOrigin: ctx.cfg.historyOrigin, ...(f ? { fetch: f } : {}) });
      ctx.secrets.push(tok);
      ctx.machineTokens.set(machineId, tok);
      if (!ctx.tokens.has(tenantKey(t))) ctx.tokens.set(tenantKey(t), tok);
      ctx.runner.note(g.record);
    } catch (e) {
      ctx.runner.note({ ...g.record, status: "failed", code: e instanceof IngestError ? e.code : "mint_error" });
      return `token mint failed for ${t.orgId}/${machineId}: ${e instanceof IngestError ? e.code : "error"}`;
    }
  }
  return undefined;
}

/**
 * Mint a token for an org the proof stage does not list and send it an empty
 * chunk under that org's prefix. The ingest must refuse it with 403
 * org_not_allowed; an empty body means even a broken check cannot store a byte.
 */
export async function unlistedOrgProbe(ctx: Ctx, relayauthUrl: string, ingestUrl: string, f: Fetch = fetch): Promise<{ status: number; code: string }> {
  const bootstrapKey = ctx.env.PROOF_RELAYAUTH_BOOTSTRAP_KEY;
  if (!bootstrapKey) return { status: 0, code: "no_bootstrap_key" };
  const g = ctx.runner.gate({ id: "ingest.unlisted-org", argv: ["HTTP", "POST", `${relayauthUrl}/v1/tokens/relayhistory-assertion`, "+", `${ingestUrl}/v1/pipeline/chunks`], mutating: true, touches: [`relayauth (1h token for unlisted ${PROOF_UNLISTED_ORG})`, "alleyoop-ingest (empty chunk, expected refusal)"], approval: "khaliq" });
  ctx.runner.note(g.record);
  if (!g.allowed) return { status: 0, code: "not_allowed_in_mode" };
  let tok: string;
  try {
    tok = await mintTenantToken({ relayauthUrl, bootstrapKey, orgId: PROOF_UNLISTED_ORG, workspaceId: "default", machineId: "proof-unlisted-m0", sponsorId: ctx.env.PROOF_SPONSOR_ID ?? "proof-operator", historyOrigin: ctx.cfg.historyOrigin, fetch: f });
  } catch (e) {
    return { status: 0, code: e instanceof IngestError ? `mint_${e.code}` : "mint_error" };
  }
  ctx.secrets.push(tok);
  const q = new URLSearchParams({ format: "jsonl", seq: "0", epoch: "0", offset: "0", date: "2026-10-31", machine: "proof-unlisted-m0", harness: "claude", session: "unlisted-0" });
  const res = await f(`${ingestUrl}/v1/pipeline/chunks?${q}`, { method: "POST", headers: { authorization: `Bearer ${tok}`, "content-type": "application/octet-stream", "x-machine-id": "proof-unlisted-m0", "x-session-id": "unlisted-0" }, body: new Uint8Array() });
  const body = (await res.json().catch(() => ({}))) as { error?: { code?: unknown }; code?: unknown };
  const code = typeof body.error?.code === "string" ? body.error.code : typeof body.code === "string" ? body.code : `http_${res.status}`;
  return { status: res.status, code };
}

async function plantRevision(ctx: Ctx, ingestUrl: string, sessions: readonly PlantedSession[], revision: 1 | 2): Promise<string[]> {
  const errors: string[] = [];
  const g = ctx.runner.gate({ id: `ingest.plant.rev${revision}`, argv: ["HTTP", "POST", `${ingestUrl}/v1/pipeline/chunks`, "+", `${ingestUrl}/v1/pipeline/manifests`, `x${sessions.length}`], mutating: true, touches: [`s3:${ctx.cfg.locker.bucket} (${sessions.length} synthetic sessions, revision ${revision})`, `hoopdb:${ctx.cfg.hoopdb.cluster} (index rows via the pipeline)`], approval: "khaliq" });
  if (!g.allowed) {
    ctx.runner.note(g.record);
    return ["planting not allowed in this mode"];
  }
  await inBatches(sessions, async (p) => {
    const st = ctx.uploads.get(p.sessionId) ?? newUploadState();
    ctx.uploads.set(p.sessionId, st);
    if (st.landed.length >= revision) return;
    try {
      const token = ctx.machineTokens.get(p.machineId);
      if (!token) throw new IngestError("no_machine_token", 0);
      await plantNextRevision({ ingestUrl }, p, token, st);
    } catch (e) {
      errors.push(`${p.sessionId}: ${e instanceof IngestError ? e.code : "plant_error"}`);
    }
  });
  ctx.runner.note({ ...g.record, status: errors.length ? "failed" : "ran", ...(errors.length ? { code: `${errors.length}_failed` } : {}) });
  return errors;
}

interface DbState {
  index: Set<string>;
  directoryPresent: boolean;
  searchRpcPresent: boolean;
  times: Map<string, { objectLanded?: number; indexed?: number; summary?: number }>;
  directory: Map<string, { short_summary: string | null; generated_at_ms: number | null; deleted: boolean; org_id: string }>;
}

function readDb(ctx: Ctx, pod: string, sessions: readonly PlantedSession[], label: string): DbState | undefined {
  const ids = sessions.map((p) => p.sessionId);
  const state = sql<{ index: Array<{ session_id: string }>; directory_present: boolean; search_rpc_present: boolean }>(ctx, `hoopdb.index.${label}`, pod, indexStateSql(ids));
  const times = sql<Array<{ trace_id: string; stage: string; at_ms: number }>>(ctx, `hoopdb.stage-times.${label}`, pod, stageTimesSql(sessions.map((p) => p.traceId)));
  if (!state || !times) return undefined;
  const byTrace = new Map(sessions.map((p) => [p.traceId, p.sessionId]));
  const t = new Map<string, { objectLanded?: number; indexed?: number; summary?: number }>();
  for (const row of times) {
    const sid = byTrace.get(row.trace_id);
    if (!sid) continue;
    const e = t.get(sid) ?? {};
    if (row.stage === "object-landed") e.objectLanded = Number(row.at_ms);
    if (row.stage === "session-indexed") e.indexed = Number(row.at_ms);
    if (row.stage === "summary-short") e.summary = Number(row.at_ms);
    t.set(sid, e);
  }
  const directory = new Map<string, { short_summary: string | null; generated_at_ms: number | null; deleted: boolean; org_id: string }>();
  if (state.directory_present) {
    const rows = sql<Array<{ session_id: string; short_summary: string | null; generated_at_ms: number | null; deleted: boolean; org_id: string }>>(ctx, `hoopdb.directory.${label}`, pod, directorySql(ids));
    for (const row of rows ?? []) directory.set(row.session_id, row);
  }
  return { index: new Set(state.index.map((x) => x.session_id)), directoryPresent: state.directory_present, searchRpcPresent: state.search_rpc_present, times: t, directory };
}

function s3Present(ctx: Ctx): Set<string> | undefined {
  const present = new Set<string>();
  const prefixes = new Set(ctx.corpus.map((p) => manifestKey(locationOf(p)).split("/h=")[0]! + "/"));
  for (const prefix of prefixes) {
    const o = ctx.runner.run(s3ListStep(ctx.cfg, prefix));
    if (!o.ok) return undefined;
    for (const obj of (JSON.parse(o.stdout || "null") as Array<{ Key: string }> | null) ?? []) present.add(obj.Key);
  }
  const out = new Set<string>();
  for (const p of ctx.corpus) if (present.has(manifestKey(locationOf(p)))) out.add(p.sessionId);
  return out;
}

/* ================================================================ 2. planted trace */

export async function plantedTrace(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "planted-trace");
  const c = ctx.cfg;
  r.metrics.planted = ctx.corpus.length;
  r.metrics.tenants = c.tenants.map(tenantKey);
  if (dry(ctx)) {
    ctx.runner.record({ id: "relayauth.mint", argv: ["HTTP", "POST", "<relayauth port-forward>/v1/tokens/relayhistory-assertion", `x${new Set(ctx.corpus.map((p) => p.machineId)).size}`], mutating: true, touches: ["relayauth (machine-bound tenant tokens)"], approval: "khaliq" });
    ctx.runner.record({ id: "ingest.plant.rev1", argv: ["HTTP", "POST", "<ingest port-forward>/v1/pipeline/chunks", "+", "/v1/pipeline/manifests", `x${ctx.corpus.length}`], mutating: true, touches: [`s3:${c.locker.bucket}`, `hoopdb:${c.hoopdb.cluster}`], approval: "khaliq" });
    for (const s of [primaryStep(c), sqlStep(c, "hoopdb.index", "", indexStateSql(ctx.corpus.map((p) => p.sessionId))), sqlStep(c, "hoopdb.stage-times", "", stageTimesSql(ctx.corpus.map((p) => p.traceId))), s3ListStep(c, "<run machine prefix>")]) ctx.runner.run(s);
    ctx.runner.run(sqlStep(c, "search.own", "", searchBatchSql(c.tenants[0]!, [{ tag: "example", q: ctx.corpus[0]!.summaryTerm }])));
    return r.done("planned", `plants ${ctx.corpus.length} UUIDv7 sessions across ${c.tenants.length} tenants, then traces S3 -> index -> short_summary -> V1 search`);
  }
  if (ctx.runner.mode === "read-only") return notRun(ctx, r, "planting writes sessions");
  const outcome = await withForwards(ctx, async (ingest, ra) => {
    const tokenIssue = await tokensFor(ctx, ra.url);
    if (tokenIssue) return tokenIssue;
    // Negative: an org the stage does not list must be refused at ingest with 403 org_not_allowed.
    const neg = await unlistedOrgProbe(ctx, ra.url, ingest.url);
    r.metrics.unlistedOrg = neg;
    if (neg.status !== 403 || neg.code !== "org_not_allowed") r.failures.push(`unlisted org ${PROOF_UNLISTED_ORG}: ingest answered ${neg.status} ${neg.code}, expected 403 org_not_allowed`);
    const errs = await plantRevision(ctx, ingest.url, ctx.corpus, 1);
    r.metrics.plantErrors = errs.length;
    if (errs.length) r.failures.push(`${errs.length} session(s) not accepted by ingest; first: ${errs.slice(0, 5).join("; ")}`);
    return undefined;
  });
  if (outcome === undefined && ctx.tokens.size === 0) return r.done("blocked", "port-forward to ingest or relayauth was not allowed");
  if (typeof outcome === "string") return r.done("blocked", outcome);
  const pod = primary(ctx);
  if (!pod) return r.done("blocked", "could not resolve the CNPG primary");
  const deadline = ctx.now() + c.stageDeadlineMs;
  let db: DbState | undefined;
  while (true) {
    db = readDb(ctx, pod, ctx.corpus, "poll");
    const done = db && ctx.corpus.every((p) => db!.times.get(p.sessionId)?.summary !== undefined && (!db!.directoryPresent || db!.directory.get(p.sessionId)?.short_summary));
    if (done || ctx.now() >= deadline) break;
    await ctx.sleep(c.pollIntervalMs);
  }
  if (!db) return r.done("blocked", "HoopDb probes failed");
  const s3 = s3Present(ctx);
  if (!s3) r.failures.push("S3 listing failed");
  // Gate evidence needs the authenticated path deployed: PostgREST in front of the RPC, plus the exchange key.
  const httpPath = c.search.mode === "postgrest-jwt" && (!!ctx.httpSearch || (ctx.runner.run({ id: "search.postgrest-service", argv: kubectl(c, "-n", PROOF_POSTGREST.namespace, "get", "service", PROOF_POSTGREST.service, "-o", "name"), mutating: false, touches: [`k8s:${PROOF_POSTGREST.namespace}/service/${PROOF_POSTGREST.service}`], approval: "operator" }).ok && !!ctx.env[SEARCH_EXCHANGE_KEY_ENV]));
  const searchChecked = db.directoryPresent && db.searchRpcPresent && (c.search.mode === "rpc-sql" || httpPath);
  r.metrics.directoryPresent = db.directoryPresent;
  r.metrics.searchRpcPresent = db.searchRpcPresent;
  r.metrics.searchMode = c.search.mode;
  const found = searchChecked ? await searchOwn(ctx, pod, ctx.corpus, "own") : new Map();
  const expected = new Map<string, string>();
  if (c.extractiveSummaries) for (const p of ctx.corpus) expected.set(p.sessionId, await expectedShortSummary(p, 1));
  const obs = new Map<string, SessionObservation>();
  for (const p of ctx.corpus) {
    const f = found.get(p.sessionId);
    obs.set(p.sessionId, {
      sessionId: p.sessionId,
      s3Present: s3?.has(p.sessionId) ?? false,
      indexRow: db.index.has(p.sessionId),
      times: db.times.get(p.sessionId) ?? {},
      shortSummary: db.directoryPresent ? db.directory.get(p.sessionId)?.short_summary ?? null : undefined,
      searchFound: f?.found,
      searchLatencyMs: f?.ms,
      searchError: f?.error,
    });
  }
  const v = evaluatePlantedTrace(ctx.corpus, obs, {
    expectedSummaries: c.extractiveSummaries ? expected : undefined,
    forbiddenInSummary: (p) => [FAKE_AWS_KEY_ID, p.transcriptCanary],
    searchChecked,
  });
  // Without session_directory the "short_summary populated" stage cannot be observed at all.
  r.failures.push(...v.failures.filter((f) => db.directoryPresent || !f.startsWith("short_summary not populated")));
  Object.assign(r.metrics, v.metrics);
  ctx.searchable = new Set([...found].filter(([, f]) => f.found).map(([id]) => id));
  if (!db.directoryPresent) return r.done(r.failures.length ? "fail" : "blocked", "session_directory.short_summary is absent in this HoopDb (#38/#48 migrations not applied); summary and search stages not observable");
  if (!searchChecked)
    return r.done(r.failures.length ? "fail" : "blocked", c.search.mode === "postgrest-jwt"
      ? "the authenticated V1 search path (RelayAuth session-directory-search exchange -> PostgREST) is not deployed or the exchange key is missing; search gate BLOCKED"
      : `V1 search not reachable (search.mode=${c.search.mode}, rpc present=${db.searchRpcPresent})`);
  return r.done(r.failures.length ? "fail" : "pass", `${v.metrics.stageCounts.search}/${ctx.corpus.length} planted sessions traced S3 -> index -> summary -> search`);
}

/* ================================================================ 3. tenant isolation + leak scan */

export async function tenantIsolation(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "tenant-isolation");
  const c = ctx.cfg;
  const [a, b] = c.tenants as [Tenant, Tenant];
  if (dry(ctx)) {
    ctx.runner.run(sqlStep(c, "isolation.search", "", searchBatchSql(a, [{ tag: "other_tenant_canary", q: ctx.corpus.find((p) => tenantKey(p.tenant) === tenantKey(b))!.tenantCanary }])));
    ctx.runner.record({ id: "isolation.by-id", argv: ["HTTP", "GET", "<ingest>/v1/pipeline/traces/<other tenant trace>", "(tenant A token)"], mutating: false, touches: ["alleyoop-ingest (read)"], approval: "operator" });
    ctx.runner.record({ id: "isolation.manifest-status", argv: ["HTTP", "GET", "<ingest>/v1/pipeline/manifests/status?key=<other tenant key>", "(tenant A token)"], mutating: false, touches: ["alleyoop-ingest (read)"], approval: "operator" });
    for (const s of logSteps(ctx)) ctx.runner.run(s);
    ctx.runner.run(traceListStep(ctx));
    return r.done("planned", "searches, by-id and manifest-status probes across tenants; scans logs and the S3 trace archive for planted canaries");
  }
  if (ctx.tokens.size < c.tenants.length) return notRun(ctx, r, "needs planted sessions and tenant tokens (run planted-trace first)");
  const pod = primary(ctx);
  const owner = new Map(ctx.corpus.map((p) => [p.sessionId, p.tenant]));
  const canaries = new Map<string, string[]>();
  for (const p of ctx.corpus) canaries.set(tenantKey(p.tenant), [...new Set([...(canaries.get(tenantKey(p.tenant)) ?? []), p.tenantCanary, p.summaryTerm])]);
  const probes: ProbeObservation[] = [];
  const tag = ctx.corpus[0]!.summaryTerm.replace(/t\d+s\d+sum$/, "");
  // Search + list: other tenants' canaries and summary terms, then the run-wide prefix.
  for (const t of c.tenants) {
    const others = ctx.corpus.filter((p) => tenantKey(p.tenant) !== tenantKey(t)).slice(0, 20);
    const qs = [...new Set(others.map((p) => p.tenantCanary))].map((q, i) => ({ tag: `canary_${i}`, q }))
      .concat(others.map((p) => ({ tag: tagOf(p, "x"), q: p.summaryTerm })), [{ tag: "list_run", q: tag }]);
    const res = await searchTenant(ctx, pod, t, qs, `isolation.search.${t.orgId}`);
    if (!res) r.failures.push(`isolation search failed for ${t.orgId}`);
    for (const line of res?.values() ?? []) probes.push({ surface: line.tag === "list_run" ? "list" : "search", caller: t, status: 200, sessionIds: line.rows.map((x) => x.session_id), body: JSON.stringify(line.rows) });
    const own = res?.get("list_run")?.rows.filter((x) => owner.get(x.session_id) && tenantKey(owner.get(x.session_id)!) === tenantKey(t)).length ?? 0;
    if (res && own === 0) r.failures.push(`positive control failed: ${t.orgId} listed none of its own sessions`);
  }
  // By id and manifest status through AlleyOop, with each tenant's token against the other's session.
  await withForwards(ctx, async (ingest) => {
    for (const t of c.tenants) {
      const tok = ctx.tokens.get(tenantKey(t))!;
      const mine = ctx.corpus.find((p) => tenantKey(p.tenant) === tenantKey(t))!;
      const theirs = ctx.corpus.filter((p) => tenantKey(p.tenant) !== tenantKey(t)).slice(0, 5);
      const own = await fetch(`${ingest.url}/v1/pipeline/traces/${mine.traceId}`, { headers: { authorization: `Bearer ${tok}` } });
      if (own.status !== 200) r.failures.push(`positive control failed: ${t.orgId} could not read its own trace (${own.status})`);
      for (const p of theirs) {
        const byId = await fetch(`${ingest.url}/v1/pipeline/traces/${p.traceId}`, { headers: { authorization: `Bearer ${tok}` } });
        const body = await byId.text();
        probes.push({ surface: "by-id", caller: t, status: byId.status, sessionIds: byId.status === 200 ? [p.sessionId] : [], body });
        if (!refusedAsExpected(byId.status)) r.failures.push(`by-id: ${t.orgId} got ${byId.status} for another tenant's trace`);
        const q = new URLSearchParams({ key: manifestKey(locationOf(p)), machine: p.machineId, writer: p.machineId });
        const ms = await fetch(`${ingest.url}/v1/pipeline/manifests/status?${q}`, { headers: { authorization: `Bearer ${tok}` } });
        probes.push({ surface: "manifest-status", caller: t, status: ms.status, sessionIds: ms.status === 200 ? [p.sessionId] : [], body: await ms.text() });
        if (!refusedAsExpected(ms.status)) r.failures.push(`manifest-status: ${t.orgId} got ${ms.status} for another tenant's manifest`);
      }
    }
  });
  const { hits, unknownIds } = crossTenantHits(probes, owner, canaries);
  for (const h of hits) r.failures.push(`cross-tenant hit on ${h.surface}: ${h.caller} saw ${h.owner} (${h.subject})`);
  r.metrics.probes = probes.length;
  r.metrics.crossTenantHits = hits.length;
  r.metrics.unknownIdsReturned = unknownIds.length;
  // Leak scan: logs and the trace archive must not carry transcript text, summary text, the fake key or a token.
  const sources = new Map<string, string>();
  for (const s of logSteps(ctx)) {
    const o = ctx.runner.run(s);
    if (o.ok) sources.set(s.id, o.stdout);
    else r.failures.push(`log collection failed: ${s.id}`);
  }
  const traces = collectTraces(ctx);
  if (traces === undefined) r.failures.push("trace archive listing failed");
  else sources.set("s3:telemetry/traces", traces);
  const forbidden = [...telemetryForbidden(ctx.corpus), ...ctx.secrets];
  const leaks = scanForCanaries(sources, forbidden);
  for (const l of leaks) r.failures.push(`planted text in ${l.source}: ${l.canary} x${l.count} (first at offset ${l.offset})`);
  r.metrics.leakSources = [...sources].map(([k, v]) => ({ source: k, bytes: v.length }));
  r.metrics.leakHits = leaks.length;
  if (traces !== undefined && traces.length === 0) r.failures.push("trace archive returned no spans for the run window; the scan proves nothing");
  return r.done(r.failures.length ? "fail" : "pass", `${probes.length} cross-tenant probes, 0 hits; ${sources.size} log/trace sources scanned clean`);
}

function logSteps(ctx: Ctx): Step[] {
  const c = ctx.cfg;
  const deploys = [...c.coreDeployments.map((d) => [c.namespace, d] as const), [c.relayauth.namespace, c.relayauth.service] as const];
  return deploys.map(([ns, d]) => ({ id: `logs.${d}`, argv: kubectl(c, "-n", ns, "logs", `deployment/${d}`, "--all-containers", "--since=3h", "--prefix"), mutating: false, touches: [`k8s:${ns}/deployment/${d} (logs)`], approval: "operator" as const, timeoutMs: 180_000 }));
}

function traceListStep(ctx: Ctx): Step {
  return { id: "traces.list", argv: ["aws", "s3api", "list-objects-v2", "--region", ctx.cfg.region, "--bucket", ctx.cfg.locker.bucket, "--prefix", "telemetry/traces/", "--query", "Contents[].{Key:Key,LastModified:LastModified}", "--output", "json"], mutating: false, touches: [`s3:${ctx.cfg.locker.bucket}/telemetry/traces/ (list)`], approval: "operator" };
}

/** Download (to memory only) every trace batch written since the run started, gunzipped. */
function collectTraces(ctx: Ctx): string | undefined {
  const list = ctx.runner.run(traceListStep(ctx));
  if (!list.ok) return undefined;
  const since = ctx.now() - 3 * 3600_000;
  const keys = ((JSON.parse(list.stdout || "null") as Array<{ Key: string; LastModified: string }> | null) ?? []).filter((o) => Date.parse(o.LastModified) >= since).map((o) => o.Key);
  let text = "";
  for (const key of keys) {
    // No shell: the batch comes back base64 and is gunzipped in process (plain text passes through).
    const o = ctx.runner.run({ id: "traces.get", argv: ["aws", "s3", "cp", "--region", ctx.cfg.region, `s3://${ctx.cfg.locker.bucket}/${key}`, "-"], mutating: false, touches: [`s3:${ctx.cfg.locker.bucket}/${key} (read)`], approval: "operator", binary: true });
    if (!o.ok) continue;
    const buf = Buffer.from(o.stdout, "base64");
    try {
      text += gunzipSync(buf).toString("utf8") + "\n";
    } catch {
      text += buf.toString("utf8") + "\n";
    }
  }
  return text;
}

/* ================================================================ 4. V1 search scope */

export async function v1SearchScope(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "v1-search-scope");
  r.metrics.fullTranscriptSearch = "UNSUPPORTED";
  r.metrics.reason = "V2 search backend (ClickHouse/ObsessionDB) absent; V1 indexes session_directory metadata + scrubbed short_summary only";
  if (dry(ctx)) {
    ctx.runner.run(sqlStep(ctx.cfg, "scope.transcript-probe", "", searchBatchSql(ctx.cfg.tenants[0]!, [{ tag: "transcript_only", q: ctx.corpus[0]!.transcriptCanary }])));
    return r.done("planned", "full-transcript search: UNSUPPORTED (V2 absent); probes that transcript-only text is not searchable");
  }
  if (!ctx.searchable) return r.done("blocked", "full-transcript search: UNSUPPORTED (V2 absent); probe needs planted-trace with V1 search reachable");
  const pod = primary(ctx);
  const probes: Array<{ hits: number; error?: string }> = [];
  for (const t of ctx.cfg.tenants) {
    const mine = ctx.corpus.filter((p) => tenantKey(p.tenant) === tenantKey(t)).slice(0, 10);
    const res = await searchTenant(ctx, pod, t, mine.map((p) => ({ tag: tagOf(p, "tx"), q: p.transcriptCanary })), `scope.transcript.${t.orgId}`);
    for (const p of mine) {
      const line = res?.get(tagOf(p, "tx"));
      probes.push(line ? { hits: line.rows.length } : { hits: 0, error: res ? "no_result_line" : "search_failed" });
    }
  }
  const v = evaluateV1Scope(probes);
  r.failures.push(...v.failures);
  r.metrics.transcriptProbes = probes.length;
  return r.done(v.status, v.status === "unsupported" ? `full-transcript search: UNSUPPORTED (V2 absent); ${probes.length} transcript-only probes returned 0 rows` : "transcript-scope probe failed");
}

/* ================================================================ transcript-search gate */

export async function transcriptSearch(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "transcript-search");
  const tc = ctx.cfg.transcriptSearch;
  const label = ctx.transcriptBackend?.label ?? backendLabel(tc);
  r.metrics.backend = label;
  r.metrics.feed = tc.feed;
  if (label === "none")
    return TRANSCRIPT_GATE_DEFERRAL.deferred
      ? r.done("deferred", `transcript-search ${TRANSCRIPT_GATE_DEFERRAL.reason}; not run, does not block the verdict, never PASS`)
      : r.done("blocked", "no transcript-search backend configured (transcriptSearch.backend=none); the proof stays INCOMPLETE");
  const insertStep: Step = { id: "transcript.feed.synthetic-insert", argv: ["HTTP", "POST", `${tc.endpoint ?? "<endpoint>"}/?query=INSERT INTO ${tc.database}.${tc.table} FORMAT JSONEachRow`, `x${ctx.corpus.length}`], mutating: true, touches: [`${label} (synthetic planted transcripts written)`], approval: "khaliq" };
  const searchStep: Step = { id: "transcript.search", argv: ["HTTP", "POST", `${tc.endpoint ?? "<endpoint>"}/?param_q=<canary>&readonly=1`, "(per-tenant credentials; no tenant predicate)"], mutating: false, touches: [`${label} (read)`], approval: "operator" };
  if (dry(ctx)) {
    if (tc.feed === "harness-synthetic") ctx.runner.record(insertStep);
    ctx.runner.record(searchStep);
    ctx.runner.record({ ...searchStep, id: "transcript.cross-tenant" });
    return r.done("planned", `${label}: own-tenant transcript search for every planted session (freshness from landed), cross-tenant probes, p50/p95/p99 + error rate`);
  }
  let backend = ctx.transcriptBackend;
  if (!backend) {
    const creds = credentialsFromEnv(tc, ctx.env);
    ctx.secrets.push(...creds.secrets);
    const missing = ctx.cfg.tenants.filter((t) => !creds.tenants.has(tenantKey(t)));
    if (missing.length) return r.done("blocked", `no transcript-search credential for ${missing.map(tenantKey).join(", ")} in $${tc.credentials.tenants}`);
    if (tc.feed === "harness-synthetic" && !creds.writer) return r.done("blocked", `synthetic feed needs $${tc.credentials.writer}`);
    backend = clickhouseBackend({ url: tc.endpoint!, database: tc.database!, table: tc.table!, tenants: creds.tenants, writer: creds.writer, label });
  }
  const landed = new Map<string, number>();
  if (tc.feed === "harness-synthetic") {
    const g = ctx.runner.gate(insertStep);
    ctx.runner.note(g.record);
    if (!g.allowed) return notRun(ctx, r, "the synthetic feed writes to the transcript backend");
    if (!backend.insert) return r.done("blocked", "backend has no writer for the synthetic feed");
    try {
      // The product scrubs before storage; the synthetic feed writes the same scrubbed text.
      await backend.insert(ctx.corpus.map((p) => ({ org_id: p.tenant.orgId, workspace_id: p.tenant.workspaceId, session_id: p.sessionId, content: p.revisions[0]!.map((l) => scrubText(l)).join("\n") })));
    } catch (e) {
      return r.done("fail", `synthetic feed insert failed (${(e as { code?: string }).code ?? "error"})`);
    }
    const at = ctx.now();
    for (const p of ctx.corpus) landed.set(p.sessionId, at);
  } else {
    for (const p of ctx.corpus) {
      const t = ctx.uploads.get(p.sessionId)?.landed[0]?.landedAtMs;
      if (t !== undefined) landed.set(p.sessionId, t);
    }
    if (landed.size === 0) return notRun(ctx, r, "pipeline feed needs planted-trace in the same invocation");
  }
  const sg = ctx.runner.gate(searchStep);
  ctx.runner.note(sg.record);
  if (!sg.allowed) return notRun(ctx, r, "transcript search not allowed in this mode");
  const obs = new Map<string, TranscriptObservation>(ctx.corpus.map((p) => [p.sessionId, { sessionId: p.sessionId, landedAtMs: landed.get(p.sessionId), queryMs: [], queryErrors: 0 }]));
  const deadline = ctx.now() + ctx.cfg.stageDeadlineMs;
  while (true) {
    const pending = ctx.corpus.filter((p) => obs.get(p.sessionId)!.searchableAtMs === undefined);
    if (!pending.length) break;
    await inBatches(pending, async (p) => {
      const o = obs.get(p.sessionId)!;
      try {
        const res = await backend.search(p.tenant, p.transcriptCanary);
        o.queryMs.push(res.serverMs ?? res.clientMs);
        if (res.rows.some((x) => x.session_id === p.sessionId)) o.searchableAtMs = ctx.now();
      } catch {
        o.queryErrors += 1;
      }
    });
    if (ctx.now() >= deadline) break;
    if (ctx.corpus.some((p) => obs.get(p.sessionId)!.searchableAtMs === undefined)) await ctx.sleep(ctx.cfg.pollIntervalMs);
  }
  // Cross-tenant: each tenant's credentials search other tenants' transcript-only canaries.
  const cross: ProbeObservation[] = [];
  for (const t of ctx.cfg.tenants) {
    for (const p of ctx.corpus.filter((x) => tenantKey(x.tenant) !== tenantKey(t)).slice(0, 20)) {
      try {
        const res = await backend.search(t, p.transcriptCanary);
        cross.push({ surface: "transcript-search", caller: t, status: 200, sessionIds: res.rows.map((x) => x.session_id), body: JSON.stringify(res.rows) });
      } catch (e) {
        cross.push({ surface: "transcript-search", caller: t, status: (e as { code?: string }).code ?? "error", sessionIds: [] });
      }
    }
  }
  const v = evaluateTranscriptGate(label, tc.feed, ctx.corpus, obs, cross);
  r.failures.push(...v.failures);
  Object.assign(r.metrics, v.metrics);
  const summary = `${label}: ${v.metrics.searchable}/${v.metrics.planted} transcripts searchable by their own tenant; ${v.metrics.crossTenantProbes} cross-tenant probes, ${v.metrics.crossTenantHits} hits`;
  if (r.failures.length) return r.done("fail", summary);
  // A row the harness wrote straight into the backend proves the backend, not the product: ingest -> index -> search.
  if (tc.feed === "harness-synthetic") return r.done("blocked", `diagnostic only (harness-synthetic insert), not gate evidence; the product ingest -> index -> transcript search path must be proven. ${summary}`);
  return r.done("pass", summary);
}

/* ================================================================ #38 HOLD: stale summary */

export async function staleSummary(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "stale-summary");
  const c = ctx.cfg;
  const chosen = c.tenants.flatMap((t) => ctx.corpus.filter((p) => tenantKey(p.tenant) === tenantKey(t)).slice(0, c.staleSessionsPerTenant));
  const invalidate = chosen.filter((_, i) => i % 2 === 0);
  const summaryKey = (p: PlantedSession) => manifestKey(locationOf(p)).replace(/manifest\.json$/, "summary/short.json.enc");
  const deleteStep = (p: PlantedSession): Step => ({ id: `stale.invalidate.${p.sessionId}`, argv: ["aws", "s3api", "delete-object", "--region", c.region, "--bucket", c.locker.bucket, "--key", summaryKey(p)], mutating: true, touches: [`s3:${c.locker.bucket}/${summaryKey(p)} (delete marker; versioned)`], approval: "khaliq" });
  r.metrics.sessions = chosen.length;
  r.metrics.invalidatedObjects = invalidate.length;
  if (dry(ctx)) {
    ctx.runner.run(deleteStep(invalidate[0]!));
    ctx.runner.record({ id: "ingest.plant.rev2", argv: ["HTTP", "POST", "<ingest>/v1/pipeline/chunks", "+", "/v1/pipeline/manifests (previousVersion)", `x${chosen.length}`], mutating: true, touches: [`s3:${c.locker.bucket} (revision 2)`], approval: "khaliq" });
    ctx.runner.run(sqlStep(c, "stale.directory", "", directorySql(chosen.map((p) => p.sessionId))));
    return r.done("planned", `re-ingests ${chosen.length} sessions (revision 2), deleting the revision-1 summary object for ${invalidate.length}; the directory and search must never serve revision 1's short_summary`);
  }
  if (!ctx.searchable || ctx.tokens.size < c.tenants.length) return notRun(ctx, r, "needs planted-trace with V1 search reachable");
  if (ctx.runner.mode === "read-only") return notRun(ctx, r, "re-ingest writes");
  const pod = primary(ctx);
  const before = readDb(ctx, pod, chosen, "stale.before");
  if (!before?.directoryPresent) return r.done("blocked", "session_directory absent");
  for (const p of invalidate) {
    const o = ctx.runner.run(deleteStep(p));
    if (!o.ok && o.record.status !== "planned") r.failures.push(`could not invalidate summary object for ${p.sessionId}: ${o.record.code}`);
  }
  const plantErr = await withForwards(ctx, async (ingest) => plantRevision(ctx, ingest.url, chosen, 2));
  if (!plantErr) return r.done("blocked", "port-forward not allowed");
  if (plantErr.length) r.failures.push(`revision 2 not accepted for ${plantErr.length} session(s)`);
  const expected2 = new Map<string, string>();
  if (c.extractiveSummaries) for (const p of chosen) expected2.set(p.sessionId, await expectedShortSummary(p, 2));
  const deadline = ctx.now() + c.stageDeadlineMs;
  const verdicts = new Map<string, ReturnType<typeof evaluateStaleSummary>>();
  while (true) {
    const now = readDb(ctx, pod, chosen, "stale.poll");
    const search = await searchOwn(ctx, pod, chosen, "stale");
    const reached = ctx.now() >= deadline;
    for (const p of chosen) {
      const b = before.directory.get(p.sessionId);
      const cur = now?.directory.get(p.sessionId);
      const o: StaleObservation = {
        sessionId: p.sessionId,
        rev1Summary: b?.short_summary ?? "",
        rev1GeneratedAtMs: b?.generated_at_ms ?? undefined,
        rev2Expected: expected2.get(p.sessionId),
        current: cur?.short_summary ?? null,
        currentGeneratedAtMs: cur?.generated_at_ms ?? undefined,
        searchSummaries: search.get(p.sessionId)?.summaries ?? [],
        deadlineReached: reached,
      };
      verdicts.set(p.sessionId, evaluateStaleSummary(o));
    }
    if (reached || [...verdicts.values()].every((v) => v.state !== "pending")) break;
    await ctx.sleep(c.pollIntervalMs);
  }
  for (const [id, v] of verdicts) if (v.state !== "ok") r.failures.push(`${id}: ${v.reason}`);
  r.metrics.outcomes = Object.fromEntries(["ok", "stale", "pending"].map((s) => [s, [...verdicts.values()].filter((v) => v.state === s).length]));
  return r.done(r.failures.length ? "fail" : "pass", `${chosen.length} re-ingested sessions; no stale short_summary served`);
}

/* ================================================================ 5. restart persistence */

export async function restartPersistence(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "restart-persistence");
  const c = ctx.cfg;
  if (dry(ctx)) {
    for (const s of [...relaycastProbeSteps(c, "start"), ...relaycastProbeSteps(c, "before"), ...rolloutRestartSteps(c), primaryStep(c), cnpgInstancesStep(c), ...failoverSteps(c, ""), ...relaycastRestartSteps(c), ...relaycastProbeSteps(c, "after")]) ctx.runner.run(s);
    ctx.runner.run(sqlStep(c, "persistence.search", "", searchBatchSql(c.tenants[0]!, [{ tag: "example", q: ctx.corpus[0]!.summaryTerm }])));
    return r.done("planned", "Relaycast roundtrip before; rollout restart of each core deployment; CNPG primary failover; Relaycast restart; every planted session searchable and Relaycast data intact after");
  }
  if (!c.probeImage) return r.done("blocked", "probeImage (digest-pinned, with bun) is required for the in-cluster Relaycast roundtrip");
  if (!ctx.searchable?.size) return notRun(ctx, r, "needs planted-trace with searchable sessions");
  for (const s of relaycastProbeSteps(c, "start")) if (!ctx.runner.run(s).ok) r.failures.push(`${s.id} did not succeed`);
  const rcBefore = ctx.runner.run(relaycastProbeSteps(c, "before")[0]!);
  const vb = relaycastVerdict(rcBefore.stdout, "before", rcBefore.record.status === "ran" || rcBefore.record.status === "failed");
  if (!vb.ok) r.failures.push(...vb.failures.map((f) => `relaycast before restart: ${f}`));
  for (const s of rolloutRestartSteps(c)) {
    const o = ctx.runner.run(s);
    if (!o.ok) r.failures.push(`${s.id}: ${o.record.status} ${o.record.code ?? ""}`.trim());
  }
  const p0 = primary(ctx);
  const inst = Number(ctx.runner.run(cnpgInstancesStep(c)).stdout.trim() || 0);
  let failover = failoverVerdict(p0, "", inst);
  if (!failover.blocked) {
    for (const s of failoverSteps(c, p0)) if (!ctx.runner.run(s).ok) r.failures.push(`${s.id} failed`);
    failover = failoverVerdict(p0, primary(ctx), inst);
  }
  r.metrics.failover = failover.reason;
  if (!failover.ok && !failover.blocked) r.failures.push(`CNPG failover: ${failover.reason}`);
  for (const s of relaycastRestartSteps(c)) if (!ctx.runner.run(s).ok) r.failures.push(`${s.id} failed`);
  const rcAfter = ctx.runner.run(relaycastProbeSteps(c, "after")[0]!);
  const va = relaycastVerdict(rcAfter.stdout, "after", rcAfter.record.status === "ran" || rcAfter.record.status === "failed");
  if (!va.ok) r.failures.push(...va.failures.map((f) => `relaycast after restart: ${f}`));
  const pod = primary(ctx);
  const baseline = ctx.corpus.filter((p) => ctx.searchable!.has(p.sessionId));
  const found = await searchOwn(ctx, pod, baseline, "after-restart");
  const lost = baseline.filter((p) => !found.get(p.sessionId)?.found);
  if (lost.length) r.failures.push(`${lost.length}/${baseline.length} planted sessions no longer searchable after restarts; first: ${lost.slice(0, 5).map((p) => p.sessionId).join(", ")}`);
  r.metrics.searchableAfter = baseline.length - lost.length;
  r.metrics.searchQueryMsAfter = summarize([...found.values()].filter((f) => f.ms !== undefined).map((f) => f.ms!));
  r.ev("relaycast-probe.txt", `${rcBefore.stdout}\n${rcAfter.stdout}`);
  if (failover.blocked) return r.done(r.failures.length ? "fail" : "blocked", failover.reason);
  return r.done(r.failures.length ? "fail" : "pass", `${baseline.length} sessions searchable after ${c.coreDeployments.length} rollout restarts and CNPG failover; Relaycast roundtrip intact`);
}

/* ================================================================ 6. upgrade / rollback */

export async function upgradeRollback(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "upgrade-rollback");
  const c = ctx.cfg;
  const hist = ctx.runner.run(helmHistoryStep(c));
  const prior = dry(ctx) || !hist.ok ? "" : String((JSON.parse(hist.stdout) as Array<{ revision: number; status: string }>).filter((h) => h.status === "deployed").at(-1)?.revision ?? "");
  if (dry(ctx)) {
    for (const s of upgradeRollbackSteps(c, "<current revision>")) ctx.runner.run(s);
    ctx.runner.run(sqlStep(c, "upgrade.search", "", searchBatchSql(c.tenants[0]!, [{ tag: "example", q: ctx.corpus[0]!.summaryTerm }])));
    return r.done("planned", `helm upgrade ${c.release} to ${c.candidateChartVersion ?? "<candidate>"} then rollback; planted sessions searchable after each`);
  }
  if (!c.candidateChartVersion) return r.done("blocked", "candidateChartVersion not set");
  if (!ctx.searchable?.size) return notRun(ctx, r, "needs planted-trace with searchable sessions");
  if (!prior) return r.done("blocked", "could not read the current helm revision");
  const baseline = ctx.corpus.filter((p) => ctx.searchable!.has(p.sessionId));
  const steps = upgradeRollbackSteps(c, prior);
  const verify = async (label: string) => {
    const found = await searchOwn(ctx, primary(ctx), baseline, label);
    const lost = baseline.filter((p) => !found.get(p.sessionId)?.found).length;
    r.metrics[`searchable.${label}`] = baseline.length - lost;
    if (lost) r.failures.push(`${lost}/${baseline.length} sessions not searchable after ${label}`);
  };
  for (const s of steps.slice(0, 2)) if (!ctx.runner.run(s).ok) r.failures.push(`${s.id} failed`);
  await verify("upgrade");
  for (const s of steps.slice(2)) if (!ctx.runner.run(s).ok) r.failures.push(`${s.id} failed`);
  await verify("rollback");
  const after = ctx.runner.run(helmHistoryStep(c));
  r.ev("helm-history.json", after.stdout);
  return r.done(r.failures.length ? "fail" : "pass", `upgraded to ${c.candidateChartVersion} and rolled back to revision ${prior}; data intact`);
}

/* ================================================================ 7. backup / restore */

export async function backupRestore(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "backup-restore");
  const c = ctx.cfg;
  const name = `proof-${ctx.runId.toLowerCase().replace(/[^a-z0-9-]/g, "").slice(-20)}`;
  const steps = backupRestoreSteps(c, name);
  if (dry(ctx)) {
    for (const s of [...steps, sqlStep(c, "restore.count", `${RESTORE_CLUSTER}-1`, stageTimesSql(ctx.corpus.slice(0, 2).map((p) => p.traceId))), ...restoreCleanupSteps(c, name)]) ctx.runner.run(s);
    return r.done("planned", "S3 versioning check; on-demand CNPG backup; recovery into a separate cluster; planted sessions counted there; RPO/RTO measured; restore cluster deleted");
  }
  const versioning = ctx.runner.run(steps[0]!);
  const versioned = versioning.ok && versioningEnabled(versioning.stdout);
  r.metrics.s3Versioning = versioned ? "Enabled" : "not enabled";
  if (!versioned) r.failures.push(`S3 versioning is not Enabled on ${c.locker.bucket}`);
  const dest = ctx.runner.run(steps[1]!);
  if (!dest.stdout.trim()) return r.done(r.failures.length ? "fail" : "blocked", "the CNPG cluster has no barmanObjectStore backup configured");
  if (ctx.runner.mode === "read-only") return notRun(ctx, r, "backup and restore create resources");
  const before = readDb(ctx, primary(ctx), ctx.corpus, "backup.source");
  // Identity = planted trace id + server timestamp, kept through the restore query.
  const source = ctx.corpus.flatMap((p) => {
    const at = before?.times.get(p.sessionId)?.objectLanded;
    return at === undefined ? [] : [{ id: p.traceId, atMs: at }];
  });
  for (const s of steps.slice(2, 4)) if (!ctx.runner.run(s).ok) r.failures.push(`${s.id} failed`);
  const statusOut = ctx.runner.run(steps[4]!);
  const status = statusOut.ok ? statusOut.stdout : "";
  const stopped = Date.parse((JSON.parse(status || "{}") as { status?: { stoppedAt?: string } }).status?.stoppedAt ?? "") || ctx.now();
  const requested = ctx.now();
  for (const s of steps.slice(5)) if (!ctx.runner.run(s).ok) r.failures.push(`${s.id} failed`);
  const ready = ctx.now();
  const restored = sql<Array<{ trace_id: string; stage: string; at_ms: number }>>(ctx, "restore.count", `${RESTORE_CLUSTER}-1`, stageTimesSql(ctx.corpus.map((p) => p.traceId)));
  const present = restored?.filter((x) => x.stage === "object-landed").map((x) => ({ id: x.trace_id, atMs: Number(x.at_ms) }));
  const cleanup = restoreCleanupSteps(c, name).map((s) => ({ id: s.id, ok: ctx.runner.run(s).ok }));
  const v = evaluateBackup({ source, restored: present, backupStoppedMs: stopped, restoreRequestedMs: requested, restoreReadyMs: ready, cleanup });
  r.failures.push(...v.failures);
  Object.assign(r.metrics, v.metrics);
  return r.done(r.failures.length ? "fail" : "pass", `restore complete: RPO ${v.metrics.rpoMs} ms, RTO ${v.metrics.rtoMs} ms, versioning ${r.metrics.s3Versioning}`);
}

/* ================================================================ 8. teardown */

export async function teardown(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "teardown");
  const c = ctx.cfg;
  r.metrics.stageDestroy = STAGE_DESTROY_COMMAND;
  const verify = teardownVerifySteps(c);
  const list = ctx.runner.run(verify[0]!);
  const gone = clusterGone(list.ok, list.stdout, c.clusterName);
  if (!dry(ctx) && gone === undefined) return r.done(ctx.runner.mode === "read-only" ? "skipped" : "blocked", "could not list EKS clusters; teardown state unknown");
  if (!gone)
    for (const s of teardownSteps(c)) {
      const o = ctx.runner.run(s);
      if (!o.ok && o.record.status === "failed") r.failures.push(`${s.id} failed`);
    }
  // Once the cluster is gone there is nothing in-cluster to list; only the account is checked.
  const byId = (prefix: string) => verify.filter((v) => v.id.startsWith(prefix));
  const k8sRuns = gone ? undefined : byId("teardown.verify.k8s.").map((v) => ctx.runner.run(v));
  const k8s = k8sRuns && { ok: k8sRuns.every((x) => x.ok), stdout: JSON.stringify({ items: k8sRuns.flatMap((x) => (x.ok ? ((JSON.parse(x.stdout || "{}") as { items?: unknown[] }).items ?? []) : [])) }) };
  const ns = gone ? undefined : ctx.runner.run(byId("teardown.verify.namespace")[0]!);
  const tagged = ctx.runner.run(byId("teardown.verify.aws-tagged")[0]!);
  if (dry(ctx)) return r.done("planned", `removes proof-labelled objects, uninstalls the release, deletes the namespace, verifies nothing labelled remains; after \`96bulls down ${c.stage}\` (Khaliq), verifies zero ${PROOF_TAG.key}=${PROOF_TAG.value} resources`);
  if (!tagged.ok) r.failures.push("tagged-resource listing failed");
  if (k8s && !k8s.ok) r.failures.push("in-cluster listing failed");
  const res = teardownResidue({ clusterGone: gone === true, k8sJson: k8s?.ok ? k8s.stdout : "{}", namespaceOut: ns?.ok ? ns.stdout : "", taggedJson: tagged.ok ? tagged.stdout : "{}" });
  r.metrics.clusterGone = gone;
  r.metrics.inClusterResidue = res.inCluster;
  r.metrics.taggedAwsResources = res.tagged.length;
  if (ctx.runner.mode === "read-only") return r.done("skipped", `read-only: ${res.inCluster.length} in-cluster proof object(s), ${res.tagged.length} tagged AWS resource(s) present; nothing removed`);
  r.failures.push(...res.inCluster.map((x) => `still present: ${x}`));
  if (gone) {
    // The stack's KMS key has a 30-day deletion window: PendingDeletion is the expected end state, not residue.
    const pending: string[] = [];
    for (const t of res.tagged.filter((x) => x.startsWith("aws arn:aws:kms:"))) {
      const arn = t.slice(4);
      const k = ctx.runner.run({ id: "teardown.verify.kms", argv: ["aws", "kms", "describe-key", "--region", c.region, "--key-id", arn, "--query", "KeyMetadata.KeyState", "--output", "text"], mutating: false, touches: [`aws:${arn} (describe)`], approval: "operator" });
      if (k.ok && k.stdout.trim() === "PendingDeletion") pending.push(arn);
    }
    r.metrics.kmsPendingDeletion = pending;
    r.failures.push(...res.tagged.filter((x) => !pending.includes(x.slice(4))).map((x) => `still present after stage destroy: ${x}`));
    return r.done(r.failures.length ? "fail" : "pass", "cluster destroyed and no resource tagged for the proof stage remains in the account");
  }
  if (r.failures.length) return r.done("fail", "proof objects remain in the cluster");
  return r.done("blocked", `in-cluster proof objects removed; ${res.tagged.length} tagged AWS resource(s) remain until \`96bulls down ${c.stage}\` (Khaliq), then rerun teardown to verify zero`);
}

/* ================================================================ teardown, post-destroy */

const NOT_FOUND = /ResourceNotFoundException/;
const BUCKET_GONE = /\(404\)|Not Found|NoSuchBucket/;

/**
 * After `bin/96bulls down aws-proof`, verify absence without a cluster:
 * describe-cluster answers ResourceNotFound, list-clusters does not name it,
 * no resource tagged for the stage remains (a KMS key in PendingDeletion is the
 * expected end state), and both proof buckets answer 404. A failed or
 * ambiguous call (403, throttling, network) is never "gone".
 */
export async function postDestroyTeardown(ctx: Ctx): Promise<CheckResult> {
  const r = new Result(ctx, "teardown");
  const c = ctx.cfg;
  const aws = (id: string, args: string[], patterns?: RegExp[]): Step => ({ id, argv: ["aws", ...args, "--region", c.region, "--output", "json"], mutating: false, touches: [`aws:${args.slice(0, 2).join(" ")} (read)`], approval: "operator", ...(patterns ? { stderrPatterns: patterns } : {}) });
  const describe = ctx.runner.run(aws("post-destroy.describe-cluster", ["eks", "describe-cluster", "--name", c.clusterName], [NOT_FOUND]));
  const list = ctx.runner.run(aws("post-destroy.list-clusters", ["eks", "list-clusters"]));
  const tagged = ctx.runner.run(aws("post-destroy.tagged", ["resourcegroupstaggingapi", "get-resources", "--tag-filters", `Key=${PROOF_TAG.key},Values=${PROOF_TAG.value}`]));
  const buckets = [...new Set([c.locker.bucket, PROOF_BACKUP_BUCKET])].map((b) => ({ b, o: ctx.runner.run(aws(`post-destroy.head-bucket.${b}`, ["s3api", "head-bucket", "--bucket", b], [BUCKET_GONE])) }));
  if (dry(ctx)) return r.done("planned", "post-destroy: describe-cluster must answer ResourceNotFound, list-clusters must not name it, no tagged resources, both buckets 404");
  const clusterGoneDescribe = describe.record.status === "failed" && describe.stderrMatches?.[0] === true;
  if (!clusterGoneDescribe) r.failures.push(describe.ok ? "describe-cluster still finds the cluster" : "describe-cluster failed without ResourceNotFound; absence unproven");
  const gone = clusterGone(list.ok, list.stdout, c.clusterName);
  if (gone !== true) r.failures.push(gone === false ? "list-clusters still names the cluster" : "list-clusters failed; absence unproven");
  for (const { b, o } of buckets) if (!(o.record.status === "failed" && o.stderrMatches?.[0] === true)) r.failures.push(o.ok ? `bucket ${b} still exists` : `head-bucket ${b} failed without 404; absence unproven`);
  if (!tagged.ok) r.failures.push("tagged-resource listing failed");
  else {
    const res = teardownResidue({ clusterGone: true, k8sJson: "{}", namespaceOut: "", taggedJson: tagged.stdout });
    const pending: string[] = [];
    for (const t of res.tagged.filter((x) => x.startsWith("aws arn:aws:kms:"))) {
      const arn = t.slice(4);
      const k = ctx.runner.run({ id: "post-destroy.kms", argv: ["aws", "kms", "describe-key", "--region", c.region, "--key-id", arn, "--query", "KeyMetadata.KeyState", "--output", "text"], mutating: false, touches: [`aws:${arn} (describe)`], approval: "operator" });
      if (k.ok && k.stdout.trim() === "PendingDeletion") pending.push(arn);
    }
    r.metrics.kmsPendingDeletion = pending;
    r.failures.push(...res.tagged.filter((x) => !pending.includes(x.slice(4))).map((x) => `still present after stage destroy: ${x}`));
  }
  r.metrics.postDestroy = true;
  return r.done(r.failures.length ? "fail" : "pass", "post-destroy: cluster, tagged resources and buckets are gone");
}

export const CHECKS: Record<CheckId, (ctx: Ctx) => Promise<CheckResult>> = {
  "fresh-install": freshInstall,
  "public-ingress": publicIngress,
  "planted-trace": plantedTrace,
  "tenant-isolation": tenantIsolation,
  "v1-search-scope": v1SearchScope,
  "transcript-search": transcriptSearch,
  "stale-summary": staleSummary,
  "restart-persistence": restartPersistence,
  "upgrade-rollback": upgradeRollback,
  "backup-restore": backupRestore,
  teardown,
};
