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
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { uuidv7 } from "../../shared/ids.ts";
import { CHECKS, newCtx } from "./checks.ts";
import { loadConfig, validateConfig, type ProofConfig } from "./config.ts";
import { Runner } from "./exec.ts";
import { fenceFor } from "./fence.ts";
import { assertProofTarget, GuardError, LOCAL_READONLY_CONTEXT, type Identity, type Mode } from "./guard.ts";
import { allCanaries } from "./corpus.ts";
import { backendLabel } from "./transcript.ts";
import { buildReport, CHECK_IDS, mergeReports, sha256, validateReport, type CheckId, type CheckResult, type ProofReport } from "./report.ts";

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

/** Observed identity: who the AWS CLI is, and which cluster the kube context points at. */
export function observeIdentity(cfg: ProofConfig, run = (argv: string[]) => spawnSync(argv[0]!, argv.slice(1), { encoding: "utf8" })): Identity {
  const sts = run(["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"]);
  const kc = run(["kubectl", "config", "view", "-o", "json"]);
  let kubeContextCluster: string | undefined;
  try {
    const conf = JSON.parse(kc.stdout ?? "{}") as { contexts?: Array<{ name: string; context: { cluster: string } }> };
    kubeContextCluster = conf.contexts?.find((x) => x.name === cfg.kubeContext)?.context.cluster;
  } catch {
    kubeContextCluster = undefined;
  }
  return { accountId: sts.status === 0 ? sts.stdout.trim() : undefined, clusterName: cfg.clusterName, kubeContext: cfg.kubeContext, kubeContextCluster };
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
  const identity: Identity =
    args.mode === "dry-run" || args.target === "kind-readonly"
      ? { clusterName: cfg.clusterName, kubeContext: cfg.kubeContext }
      : observeIdentity(cfg);
  if (args.mode !== "dry-run" && args.target === "proof") {
    try {
      assertProofTarget({ ...identity, confirmLive: args.confirmLive });
    } catch (e) {
      console.error(`aws-proof: ${e instanceof GuardError ? `${e.code}: ` : ""}${(e as Error).message}`);
      return 2;
    }
  }
  // The prior report is read and validated before anything runs, so a bad merge input never costs a live run.
  let prior: { report: ProofReport; sha256: string } | undefined;
  if (args.priorReport) {
    try {
      const text = readFileSync(args.priorReport, "utf8");
      const report = JSON.parse(text) as ProofReport;
      const bad = validateReport(report);
      if (bad.length) throw new Error(bad[0]);
      prior = { report, sha256: sha256(text) };
    } catch (e) {
      console.error(`aws-proof: --prior-report unusable: ${(e as Error).message}`);
      return 2;
    }
  }
  const runId = `aws-proof-${uuidv7()}`;
  const runner = new Runner({ mode: args.mode, identity: { ...identity, confirmLive: args.confirmLive }, fence: fenceFor(cfg) });
  const ctx = newCtx(cfg, runner, runId, env);
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
  const metadataBackend = cfg.search.mode === "absent" ? "none" : `HoopDb Postgres (CNPG ${cfg.namespace}/${cfg.hoopdb.cluster}, ${cfg.search.mode === "rpc-sql" ? "sessions.search_session_directory via psql as the Worker role" : `PostgREST ${cfg.search.url}`})`;
  const transcriptBackend = ctx.transcriptBackend?.label ?? backendLabel(cfg.transcriptSearch);
  // A dry run exercises nothing; say so rather than implying the backend was hit.
  const exercised = (b: string) => (args.mode === "dry-run" && b !== "none" ? `planned, not exercised: ${b}` : b);
  const own = buildReport({ runId, mode: args.mode, backends: { metadata: exercised(metadataBackend), transcript: exercised(transcriptBackend) }, target: { stage: cfg.stage, clusterName: cfg.clusterName, kubeContext: cfg.kubeContext, region: cfg.region, ...(identity.accountId ? { accountId: identity.accountId } : {}) }, checks: results });
  let report = own;
  if (prior) {
    const merged = mergeReports(prior.report, prior.sha256, own);
    if ("error" in merged) {
      console.error(`aws-proof: cannot merge --prior-report: ${merged.error}`);
      return 2;
    }
    report = merged.report;
    console.log(`merged prior report ${prior.report.runId} (sha256 ${prior.sha256.slice(0, 12)}…); verdict covers ${report.phases.length} phase(s)`);
  }
  const problems = validateReport(report, { canaries: allCanaries(ctx.corpus), secrets: ctx.secrets });
  const dir = args.out ?? join(ROOT, ".96bulls/aws-proof", runId);
  mkdirSync(dir, { recursive: true });
  if (problems.length) {
    // Never write a report that fails its own secret/canary self-check.
    console.error(`aws-proof: report failed validation; not written:\n  ${problems.join("\n  ")}`);
    return 1;
  }
  writeFileSync(join(dir, "report.json"), JSON.stringify(report, null, 2) + "\n", { mode: 0o600 });
  console.log(`\n${report.scope.reason}`);
  console.log(`search gates: metadata=${report.searchGates.metadata.backend}; transcript=${report.searchGates.transcript.backend}`);
  console.log(`verdict: ${report.verdict.toUpperCase()}  (${report.checks.map((r) => `${r.check}=${r.status}`).join(" ")})`);
  console.log(`report: ${join(dir, "report.json")}`);
  return report.verdict === "pass" ? 0 : report.verdict === "fail" ? 1 : 3;
}

if (import.meta.main) process.exit(await main(process.argv.slice(2)));
