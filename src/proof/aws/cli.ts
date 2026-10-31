#!/usr/bin/env bun
/**
 * Isolated AWS self-host E2E proof harness (docs/aws-e2e-proof.md).
 *
 *   bun run src/proof/aws/cli.ts [checks...|all] [flags]
 *
 *   (default)          dry run: print every exact command; touch nothing
 *   --read-only        run non-mutating steps only
 *   --confirm-live     required for any step against AWS; without --read-only
 *                      it also runs mutating steps. The account and cluster are
 *                      verified first and the run refuses anything but the proof
 *                      stage (account 500268966557, cluster 96bulls-aws-proof).
 *   --target kind-readonly   the local kind-96bulls-e2e cluster; never mutated
 *   --inputs <path>    non-secret inputs (default .96bulls/aws-proof/inputs.json)
 *   --sessions <n>     planted sessions (default 100)
 *   --out <dir>        report directory (default .96bulls/aws-proof/<run id>/)
 *   --prior-report <path>  merge an earlier report of this proof (same target);
 *                      this run's checks replace the prior's, the verdict covers both
 *
 * Secrets come only from the environment, piped from 1Password for the duration
 * of the command: PROOF_RELAYAUTH_BOOTSTRAP_KEY, and the transcript backend's
 * credentials under the variable names inputs.json gives (defaults
 * PROOF_TRANSCRIPT_TENANT_CREDENTIALS, PROOF_TRANSCRIPT_WRITER).
 * Exit code: 0 pass, 1 fail, 2 usage/guard refusal, 3 incomplete.
 */
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { uuidv7 } from "../../shared/ids.ts";
import { CHECKS, newCtx } from "./checks.ts";
import { loadConfig, validateConfig, type ProofConfig } from "./config.ts";
import { Runner } from "./exec.ts";
import { fenceFor, PROOF_CHART_REPO, PROOF_CONTEXT, PROOF_POSTGREST, PROOF_REGION } from "./fence.ts";
import { bindKubeconfig, parseDescribeCluster, proofKubeconfig, toolEnv, type DescribedCluster } from "./cluster-binding.ts";
import { bindStagePlan, chartDigests, gitProvenance, loadStagePlan, sha256Hex, type ReportProvenance, type StagePlan } from "./provenance.ts";
import { assertProofTarget, GuardError, LOCAL_READONLY_CONTEXT, PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_STAGE, type Identity, type Mode } from "./guard.ts";
import { allCanaries } from "./corpus.ts";
import { backendLabel } from "./transcript.ts";
import { admitPrior, buildReport, canonicalReportBytes, CHECK_IDS, mergeReports, validateReport, type AdmittedPrior, type CheckId, type CheckResult, type ProofReport } from "./report.ts";

const ROOT = fileURLToPath(new URL("../../..", import.meta.url));

export interface Args {
  checks: CheckId[];
  mode: Mode;
  confirmLive: boolean;
  target: "proof" | "kind-readonly";
  inputs?: string;
  sessions?: number;
  out?: string;
  /** A previous report of this proof (e.g. pre-teardown) to merge into this run's report. */
  priorReport?: string;
}

export function parseArgs(argv: readonly string[]): Args {
  const checks: CheckId[] = [];
  let readOnly = false;
  let confirmLive = false;
  let target: Args["target"] = "proof";
  let inputs: string | undefined;
  let sessions: number | undefined;
  let out: string | undefined;
  let priorReport: string | undefined;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    const val = () => {
      const v = argv[++i];
      if (v === undefined || v.startsWith("--")) throw new Error(`${a} needs a value`);
      return v;
    };
    if (a === "--read-only") readOnly = true;
    else if (a === "--confirm-live") confirmLive = true;
    else if (a === "--target") {
      const t = val();
      if (t !== "proof" && t !== "kind-readonly") throw new Error(`unknown target ${t}`);
      target = t;
    } else if (a === "--inputs") inputs = val();
    else if (a === "--sessions") {
      sessions = Number(val());
      if (!Number.isInteger(sessions) || sessions < 2 || sessions > 10_000) throw new Error("--sessions must be an integer in [2, 10000]");
    } else if (a === "--out") out = val();
    else if (a === "--prior-report") priorReport = val();
    else if (a === "all") checks.push(...CHECK_IDS);
    else if ((CHECK_IDS as readonly string[]).includes(a)) checks.push(a as CheckId);
    else throw new Error(`unknown argument ${a}`);
  }
  if (target === "kind-readonly" && confirmLive && !readOnly) throw new Error("kind-readonly never runs mutating steps; drop --confirm-live or add --read-only");
  const mode: Mode = readOnly ? "read-only" : confirmLive ? "live" : "dry-run";
  return { checks: checks.length ? [...new Set(checks)] : [...CHECK_IDS], mode, confirmLive, target, inputs, sessions, out, ...(priorReport ? { priorReport } : {}) };
}

type Run = (argv: string[]) => { stdout: string; status: number | null };
const defaultRun: Run = (argv) => {
  const r = spawnSync(argv[0]!, argv.slice(1), { encoding: "utf8", shell: false, env: argv[0] === "aws" ? toolEnv(process.env, undefined) : process.env });
  return { stdout: r.stdout ?? "", status: r.status };
};

/**
 * Observed identity, bound to live AWS: who the AWS CLI is (sts), what the
 * proof cluster is (describe-cluster), and whether the operator's kube context
 * reaches exactly that endpoint with exactly that CA. These four reads are the
 * only commands that run before the guard; none mutates anything.
 */
export function observeIdentity(cfg: ProofConfig, run: Run = defaultRun): { identity: Identity; cluster?: DescribedCluster; bindError?: string } {
  const sts = run(["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"]);
  const accountId = sts.status === 0 ? sts.stdout.trim() : undefined;
  const base: Identity = { accountId, clusterName: cfg.clusterName, kubeContext: cfg.kubeContext, clusterBound: false };
  // Only ask AWS about the cluster once the caller is the proof account.
  if (accountId !== PROOF_ACCOUNT_ID) return { identity: base, bindError: "caller is not the proof account" };
  const d = run(["aws", "eks", "describe-cluster", "--name", PROOF_CLUSTER, "--region", PROOF_REGION, "--output", "json"]);
  if (d.status !== 0) return { identity: base, bindError: "describe-cluster failed" };
  const cluster = parseDescribeCluster(d.stdout);
  if ("error" in cluster) return { identity: base, bindError: cluster.error };
  const view = run(["kubectl", "config", "view", "--raw", "-o", "json"]);
  const bound = view.status === 0 ? bindKubeconfig(view.stdout, cfg.kubeContext, cluster) : { error: "kubectl config view failed" };
  // The cluster ARN comes from AWS, not from the kubeconfig's naming.
  const identity: Identity = { ...base, kubeContextCluster: cluster.arn, clusterBound: "ok" in bound };
  return { identity, cluster, ...("error" in bound ? { bindError: bound.error } : {}) };
}

export async function main(argv: readonly string[], env: Record<string, string | undefined> = process.env): Promise<number> {
  let args: Args;
  try {
    args = parseArgs(argv);
  } catch (e) {
    console.error(`aws-proof: ${(e as Error).message}`);
    return 2;
  }
  const cfg = loadConfig(args.inputs ?? join(ROOT, ".96bulls/aws-proof/inputs.json"), args.target);
  if (args.sessions) cfg.sessions = args.sessions;
  const issues = validateConfig(cfg);
  if (issues.length) {
    console.error(`aws-proof: invalid inputs:\n  ${issues.join("\n  ")}`);
    return 2;
  }
  if (args.target === "kind-readonly") {
    if (cfg.kubeContext !== LOCAL_READONLY_CONTEXT) {
      console.error(`aws-proof: kind-readonly target must use ${LOCAL_READONLY_CONTEXT}`);
      return 2;
    }
    const notReadable = args.checks.filter((c) => c !== "fresh-install");
    if (args.mode !== "dry-run" && notReadable.length) {
      console.error(`aws-proof: on ${LOCAL_READONLY_CONTEXT} only fresh-install runs; refusing ${notReadable.join(", ")}`);
      return 2;
    }
  }
  // Full prior admissibility (bytes, digest, exact target, timestamps, live phase, executed proof)
  // is decided here, before any AWS call or step: an inadmissible prior can never reach a teardown step.
  let prior: AdmittedPrior | undefined;
  if (args.priorReport) {
    if (args.mode !== "live" || args.target !== "proof") {
      console.error("aws-proof: --prior-report merges live proof runs only (needs --confirm-live without --read-only)");
      return 2;
    }
    let bytes: string;
    try {
      bytes = readFileSync(args.priorReport, "utf8");
    } catch {
      console.error("aws-proof: --prior-report unreadable");
      return 2;
    }
    const admitted = admitPrior(bytes, { stage: PROOF_STAGE, clusterName: PROOF_CLUSTER, kubeContext: PROOF_CONTEXT, accountId: PROOF_ACCOUNT_ID, region: PROOF_REGION }, Date.now());
    if ("error" in admitted) {
      console.error(`aws-proof: --prior-report refused before any step ran: ${admitted.error}`);
      return 2;
    }
    prior = admitted;
  }
  // The captured Stage A create-only plan. Live runs on the proof target refuse without it.
  const plan = args.target === "proof" ? loadStagePlan(ROOT) : undefined;
  if (args.mode !== "dry-run" && args.target === "proof" && (!plan || "error" in plan)) {
    console.error(`aws-proof: refusing: ${plan && "error" in plan ? plan.error : "no stage plan"}`);
    return 2;
  }
  const stagePlan = plan && !("error" in plan) ? plan : undefined;
  if (stagePlan && cfg.probeImage && !cfg.probeImage.endsWith(`@${stagePlan.servicesDigest}`)) {
    console.error("aws-proof: refusing: probeImage is not the stage plan's services image");
    return 2;
  }
  let identity: Identity = { clusterName: cfg.clusterName, kubeContext: cfg.kubeContext };
  let cluster: DescribedCluster | undefined;
  if (args.mode !== "dry-run" && args.target === "proof") {
    const o = observeIdentity(cfg);
    identity = o.identity;
    cluster = o.cluster;
    if (o.bindError) console.error(`aws-proof: cluster binding: ${o.bindError}`);
    try {
      assertProofTarget({ ...identity, confirmLive: args.confirmLive });
    } catch (e) {
      console.error(`aws-proof: ${e instanceof GuardError ? `${e.code}: ` : ""}${(e as Error).message}`);
      return 2;
    }
    const b = bindStagePlan(stagePlan!, cluster!);
    if ("error" in b) {
      console.error(`aws-proof: refusing: ${b.error}`);
      return 2;
    }
  }
  const runId = `aws-proof-${uuidv7()}`;
  const dir = args.out ?? join(ROOT, ".96bulls/aws-proof", runId);
  mkdirSync(dir, { recursive: true });
  // Every kubectl/helm step uses this kubeconfig, generated from describe-cluster; the operator's is never consulted.
  let kubeconfigPath: string | undefined;
  if (cluster) {
    kubeconfigPath = join(dir, "kubeconfig.json");
    writeFileSync(kubeconfigPath, proofKubeconfig(cluster, cfg.kubeContext), { mode: 0o600 });
  }
  const runner = new Runner({ mode: args.mode, identity: { ...identity, confirmLive: args.confirmLive }, fence: fenceFor(args.target), ...(kubeconfigPath ? { kubeconfigPath } : {}) });
  const ctx = newCtx(cfg, runner, runId, env);
  if (stagePlan) ctx.stagePlan = stagePlan;
  console.log(`aws-proof ${runId}: mode=${args.mode} target=${args.target} context=${cfg.kubeContext} checks=${args.checks.join(",")}`);
  if (args.mode === "dry-run") console.log("DRY RUN: nothing below runs. MUTATE* steps need --confirm-live and Khaliq's direct OK.");
  const results: CheckResult[] = [];
  for (const id of CHECK_IDS.filter((c) => args.checks.includes(c))) {
    try {
      results.push(await CHECKS[id](ctx));
    } catch (e) {
      const now = new Date().toISOString();
      const code = e instanceof GuardError ? e.code : "harness_error";
      results.push({ check: id, status: "fail", startedAt: now, finishedAt: now, summary: `harness error (${code})`, metrics: {}, failures: [`harness error: ${code}`], evidence: [], steps: [] });
    }
  }
  const metadataBackend =
    cfg.search.mode === "absent" ? "none"
    : cfg.search.mode === "rpc-sql" ? `diagnostic only: HoopDb Postgres (CNPG ${cfg.namespace}/${cfg.hoopdb.cluster}) via psql SET ROLE; not gate evidence`
    : `HoopDb Postgres via PostgREST ${PROOF_POSTGREST.namespace}/${PROOF_POSTGREST.service} with RelayAuth session-directory-search tokens`;
  const transcriptBackend = ctx.transcriptBackend?.label ?? backendLabel(cfg.transcriptSearch);
  // A dry run exercises nothing; say so rather than implying the backend was hit.
  const exercised = (b: string) => (args.mode === "dry-run" && b !== "none" ? `planned, not exercised: ${b}` : b);
  if (kubeconfigPath) rmSync(kubeconfigPath, { force: true });
  const provenance = await reportProvenance(cfg, args, stagePlan);
  const own = buildReport({ runId, mode: args.mode, provenance, backends: { metadata: exercised(metadataBackend), transcript: exercised(transcriptBackend) }, target: { stage: cfg.stage, clusterName: cfg.clusterName, kubeContext: cfg.kubeContext, region: cfg.region, ...(identity.accountId ? { accountId: identity.accountId } : {}) }, checks: results });
  let report = own;
  if (prior) {
    const merged = mergeReports(prior, own);
    if ("error" in merged) {
      console.error(`aws-proof: cannot merge --prior-report: ${merged.error}`);
      return 2;
    }
    report = merged.report;
    console.log(`merged prior report ${prior.report.runId} (sha256 ${prior.sha256}); verdict covers ${report.phases.length} phase(s)`);
  }
  const problems = validateReport(report, { canaries: allCanaries(ctx.corpus), secrets: ctx.secrets });
  if (problems.length) {
    // Never write a report that fails its own secret/canary self-check.
    console.error(`aws-proof: report failed validation; not written:\n  ${problems.join("\n  ")}`);
    return 1;
  }
  writeFileSync(join(dir, "report.json"), canonicalReportBytes(report), { mode: 0o600 });
  console.log(`\n${report.scope.reason}`);
  console.log(`search gates: metadata=${report.searchGates.metadata.backend}; transcript=${report.searchGates.transcript.backend}`);
  console.log(`verdict: ${report.verdict.toUpperCase()}  (${report.checks.map((r) => `${r.check}=${r.status}`).join(" ")})`);
  console.log(`report: ${join(dir, "report.json")}`);
  return report.verdict === "pass" ? 0 : report.verdict === "fail" ? 1 : 3;
}

if (import.meta.main) process.exit(await main(process.argv.slice(2)));

/** Report provenance. Chart digests are read from the public chart index only on non-dry runs. */
async function reportProvenance(cfg: ProofConfig, args: Args, plan: StagePlan | undefined): Promise<ReportProvenance> {
  const inputsPath = args.inputs ?? join(ROOT, ".96bulls/aws-proof/inputs.json");
  const inputsBytes = existsSync(inputsPath) ? readFileSync(inputsPath) : undefined;
  const wanted = plan
    ? Object.entries(plan.chartVersions).map(([name, version]) => ({ name, version }))
    : [{ name: "96bulls", version: cfg.chartVersion }];
  let charts: ReportProvenance["charts"] = wanted.map((w) => ({ ...w, digest: null }));
  if (args.mode !== "dry-run") {
    try {
      const res = await fetch(`${PROOF_CHART_REPO}/index.yaml`);
      if (res.ok) charts = chartDigests(await res.text(), wanted);
    } catch {
      /* digests stay null: the verdict cannot pass */
    }
  }
  return {
    harness: gitProvenance((argv) => {
      const r = spawnSync(argv[0]!, argv.slice(1), { cwd: ROOT, encoding: "utf8", shell: false });
      return { stdout: r.stdout ?? "", status: r.status };
    }),
    inputs: inputsBytes ? { sha256: sha256Hex(inputsBytes), source: "file" } : { sha256: sha256Hex(JSON.stringify(cfg)), source: "defaults" },
    configSha256: sha256Hex(JSON.stringify(cfg)),
    stagePlan: plan ? { provenanceSha256: plan.provenanceSha256, generatedAt: plan.generatedAt, commit: plan.commit, planSha256: plan.planSha256 } : null,
    charts,
  };
}
