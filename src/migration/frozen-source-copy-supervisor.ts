/**
 * Supervisor attestation of the P1 frozen-source copy (docs/p1-frozen-source-custody.md, "Supervisor").
 *
 * The qualified supervisor is the protected workflow .github/workflows/frozen-source-custody.yml, never a person or
 * a DB login. COPIED, SEALED and VERIFIED need a SupervisorProof, and only verifySupervisor() can mint one: the
 * brand is a module-private symbol plus a WeakSet of minted objects, so a hand-built or copied object is rejected.
 *
 * verifySupervisor re-reads everything live through the GitHub API with the job's GITHUB_TOKEN (never a file from a
 * previous job): fixed repository id + workflow path, workflow_dispatch on refs/heads/main, run_attempt 1, actor
 * AND triggering_actor = the pinned dispatcher, source sha = live main head, the protected environment (reviewers
 * or pinned dispatcher, single `main` branch policy, human approval, a deployment of this run), the full default
 * push proof of scripts/schema-release-github.ts green at that head (ci.yml jobs `checks` + `session-read-worker /
 * worker`, pg-proof.yml jobs `real PostgreSQL 15/16/17`), an exact-source plan-mode run under 24h old, and the typed
 * approval equal (exact string) to the one this operator derives from the live source/target state. Refusal codes:
 *   frozen_copy_supervisor_missing      no Actions context / token / approval / plan run / proof
 *   frozen_copy_supervisor_unqualified  wrong repository, workflow, event, ref, attempt, actor, environment, CI or plan run
 *   frozen_copy_supervisor_stale        main moved past the source sha, or the plan run is older than 24h
 *   frozen_copy_supervisor_mismatch     sha / mode / approval field / manifest differs from what is bound
 * detail.check names the failed check (no value). The receipt carries only the sanitized `block`.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { SCHEMA_RELEASE } from "../hoopdb/schema-release-contract.ts";
import {
  type Get, isPinnedDispatcher, requireGreenPushRuns, requireHumanApproval, requireProtectedEnvironment,
} from "../../scripts/schema-release-github.ts";
import { FROZEN_COPY_NEON_PIN, FrozenCopyRefusal, type CutoverStatus } from "./frozen-source-copy-seal.ts";
import { operatorSha256, type CopyBinding } from "./frozen-source-copy-progress.ts";

const sha = (t: string) => createHash("sha256").update(t).digest("hex");
type Obj = Record<string, any>;

export type CustodyAction = "copy" | "seal" | "readback";
export const FROZEN_COPY_RECEIPT_SCHEMA = "frozen-source-copy-job-v3";
export const FROZEN_CUSTODY = {
  repositoryId: "1437714333",
  repository: SCHEMA_RELEASE.repository,
  workflow: ".github/workflows/frozen-source-custody.yml",
  environment: "frozen-source-custody-production",
  dispatcher: SCHEMA_RELEASE.dispatcher,
  planMaxAgeMs: 24 * 60 * 60 * 1000,
} as const;
/** The workflow's operator step, verbatim (pinned by test against the YAML and the doc). Its sha256 is bound. */
export const FROZEN_COPY_CREDENTIAL_PRODUCER =
  `printf '%s' "$FROZEN_COPY_CREDENTIALS" | bun scripts/frozen-source-copy.ts "--$CUSTODY_MODE" --page 5000 --max-pages 0 --budget-ms 19800000`;
export const FROZEN_COPY_CREDENTIAL_PRODUCER_SHA256 = sha(FROZEN_COPY_CREDENTIAL_PRODUCER);

/** The operator's own source (driver + every module it executes for custody decisions), repository-relative. */
export const FROZEN_COPY_OPERATOR_FILES = [
  "src/migration/frozen-source-copy-seal.ts",
  "src/migration/frozen-source-copy-progress.ts",
  "src/migration/frozen-source-copy-queries.ts",
  "src/migration/frozen-source-copy-supervisor.ts",
  "scripts/schema-release-github.ts",
  "scripts/frozen-source-copy.ts",
] as const;
export function frozenCopyOperatorSha256(): string {
  return operatorSha256(FROZEN_COPY_OPERATOR_FILES.map((f) => ({ name: f, bytes: readFileSync(new URL("../../" + f, import.meta.url)) })));
}

/** Source = the pinned Neon owner identity (endpoint digest, user, database) and the frozen cutover descriptor. */
export function sourceDescriptorSha256(status: CutoverStatus): string {
  return sha(JSON.stringify(["frozen-copy-source-v1", FROZEN_COPY_NEON_PIN.endpointSha256, FROZEN_COPY_NEON_PIN.user, FROZEN_COPY_NEON_PIN.database,
    String(status.phase), String(status.epoch), String(status.privacyBaselineSha256)]));
}
/** Target = the pinned schema-release owner database and the live target policy (the copy binding). */
export function targetDescriptorSha256(b: Omit<CopyBinding, "operatorSha256">): string {
  return sha(JSON.stringify(["frozen-copy-target-v1", SCHEMA_RELEASE.host, SCHEMA_RELEASE.database, b.generation, b.sourceIdentitySha256, b.controlEpoch, b.privacyBaselineSha256]));
}

/** What this operator run derives itself; the approval must spell exactly these values. */
export interface SupervisorFacts {
  action: CustodyAction;
  source: string;
  target: string;
  epoch: string;
  privacy: string;
  generation: string;
  /** copy/readback: null ("none"); seal: null here, the approved manifest is enforced inside sealFence. */
  manifest: string | null;
  operator: string;
  page: number;
  maxPages: number;
  budgetMs: number;
}
const APPROVAL_FIELDS = ["FROZEN-COPY", "SOURCE", "TARGET", "EPOCH", "PRIVACY", "GENERATION", "MANIFEST", "OPERATOR", "PRODUCER", "RECEIPT", "PAGE", "MAXPAGES", "BUDGET", "PLAN"] as const;
/** The typed approval grammar: one line, single spaces, compared by exact equality. */
export function approvalText(f: SupervisorFacts & { sha: string; planRun: string }): string {
  const v = [`${f.action.toUpperCase()} ${f.sha}`, f.source, f.target, f.epoch, f.privacy, f.generation, f.manifest ?? "none", f.operator,
    FROZEN_COPY_CREDENTIAL_PRODUCER_SHA256, FROZEN_COPY_RECEIPT_SCHEMA, String(f.page), String(f.maxPages), String(f.budgetMs), f.planRun];
  return APPROVAL_FIELDS.map((k, i) => `${k} ${v[i]}`).join(" ");
}
/** Field-wise comparison so the refusal names the first differing field (never a value). */
function approvalDiff(got: string, want: string): string | null {
  if (got === want) return null;
  const g = got.split(" "), w = want.split(" ");
  const i = w.findIndex((t, k) => g[k] !== t);
  if (i < 0) return "approval_format";
  if (i <= 1) return "approval_action";
  if (i === 2) return "approval_sha";
  // After "FROZEN-COPY <ACTION> <sha>" the tokens alternate: label, value.
  return "approval_" + w[i % 2 ? i : i - 1]!.toLowerCase();
}

/** Sanitized attestation for the receipt: ids, logins, sha and digests only. */
export interface SupervisorBlock {
  repository_id: string;
  workflow: string;
  environment: string;
  run_id: string;
  run_attempt: 1;
  source_sha: string;
  actor: string;
  triggering_actor: string;
  plan_run: string;
  approval_sha256: string;
  action: CustodyAction;
  ci_conclusion: "success";
}
const BRAND: unique symbol = Symbol("frozen-copy-supervisor-proof");
const minted = new WeakSet<object>();
export interface SupervisorProof {
  readonly [BRAND]: true;
  readonly action: CustodyAction;
  /** Approved MANIFEST field: the seal fence requires its in-fence manifest to equal it. */
  readonly manifest: string | null;
  readonly epoch: string;
  readonly privacy: string;
  readonly block: Readonly<SupervisorBlock>;
}
export function isSupervisorProof(x: unknown): x is SupervisorProof {
  return typeof x === "object" && x !== null && minted.has(x) && (x as { [BRAND]?: unknown })[BRAND] === true;
}
/** missing unless a minted proof; mismatch unless it attests exactly `action`. */
export function supervisorProofRefusal(x: unknown, action: CustodyAction): FrozenCopyRefusal | null {
  if (!isSupervisorProof(x)) return new FrozenCopyRefusal("frozen_copy_supervisor_missing", { check: "proof" });
  if (x.action !== action) return new FrozenCopyRefusal("frozen_copy_supervisor_mismatch", { check: "action" });
  return null;
}

const no = (kind: "missing" | "unqualified" | "stale" | "mismatch", check: string): never => {
  throw new FrozenCopyRefusal(`frozen_copy_supervisor_${kind}`, { check });
};
const HEX40 = /^[0-9a-f]{40}$/, RUN_ID = /^[1-9][0-9]{0,18}$/;
type Env = Record<string, string | undefined>;

/**
 * Synchronous context check (before any connection): this is a first-attempt dispatch of the pinned workflow on main
 * by the pinned dispatcher, for `mode`, at sha CUSTODY_SHA. Every value is re-proven through the API afterwards.
 */
export function supervisorContextRefusal(env: Env, mode: CustodyAction | "plan"): FrozenCopyRefusal | null {
  try {
    if (env.GITHUB_ACTIONS !== "true") no("missing", "actions");
    if (!env.GITHUB_TOKEN) no("missing", "token");
    if (!RUN_ID.test(env.GITHUB_RUN_ID ?? "")) no("missing", "run_id");
    if (mode !== "plan" && !RUN_ID.test(env.CUSTODY_PLAN_RUN ?? "")) no("missing", "plan_run");
    if (mode !== "plan" && !env.CUSTODY_APPROVAL) no("missing", "approval");
    if (env.GITHUB_REPOSITORY !== FROZEN_CUSTODY.repository || env.GITHUB_REPOSITORY_ID !== FROZEN_CUSTODY.repositoryId) no("unqualified", "repository");
    if (env.GITHUB_EVENT_NAME !== "workflow_dispatch") no("unqualified", "event");
    if (env.GITHUB_REF !== "refs/heads/main") no("unqualified", "ref");
    if (env.GITHUB_WORKFLOW_REF !== `${FROZEN_CUSTODY.repository}/${FROZEN_CUSTODY.workflow}@refs/heads/main`) no("unqualified", "workflow_ref");
    if (env.GITHUB_RUN_ATTEMPT !== "1") no("unqualified", "run_attempt");
    if (env.GITHUB_ACTOR !== FROZEN_CUSTODY.dispatcher.login) no("unqualified", "actor");
    if (env.GITHUB_TRIGGERING_ACTOR !== FROZEN_CUSTODY.dispatcher.login) no("unqualified", "triggering_actor");
    if (env.CUSTODY_MODE !== mode) no("mismatch", "mode");
    if (!HEX40.test(env.CUSTODY_SHA ?? "") || env.GITHUB_SHA !== env.CUSTODY_SHA) no("mismatch", "sha");
    return null;
  } catch (e) {
    return e as FrozenCopyRefusal;
  }
}

/** A helper refusal (fixed code) becomes `kind` with that check; a GitHub transport/token failure is missing. */
async function as<T>(kind: "unqualified" | "stale", check: string, f: () => Promise<T>): Promise<T> {
  try {
    return await f();
  } catch (e) {
    if (e instanceof FrozenCopyRefusal) throw e;
    const code = String((e as { code?: unknown })?.code ?? "");
    if (code.startsWith("release_github")) no("missing", "github_api");
    return no(kind, check);
  }
}
/** One API read; any transport, token or HTTP failure means the attestation cannot be proven (missing). */
async function get1(get: Get, path: string): Promise<any> {
  try {
    return await get(path);
  } catch {
    return no("missing", "github_api");
  }
}
const titled = (run: Obj, mode: string, sourceSha: string) => run.display_title === `frozen-source-custody ${mode} ${sourceSha}`;
const fromThisWorkflow = (run: Obj) =>
  run.event === "workflow_dispatch" && run.head_branch === "main" && run.path === FROZEN_CUSTODY.workflow &&
  String(run.repository?.id) === FROZEN_CUSTODY.repositoryId && run.run_attempt === 1 && isPinnedDispatcher(run, FROZEN_CUSTODY.dispatcher);

/**
 * The GitHub half (also the prerequisite gate, protectedJob=false, which has no environment yet). In the protected
 * job it additionally proves the environment approval and this run's deployment to the environment.
 */
export async function verifyCustodyGithub(env: Env, get: Get, o: { protectedJob: boolean }): Promise<{ planRun: string | null }> {
  const mode = env.CUSTODY_MODE as CustodyAction | "plan";
  if (env.GITHUB_ACTIONS !== "true") no("missing", "actions");
  if (!["plan", "copy", "seal", "readback"].includes(mode)) no("mismatch", "mode");
  const ctx = supervisorContextRefusal(env, mode);
  if (ctx) throw ctx;
  const source = env.CUSTODY_SHA!, runId = env.GITHUB_RUN_ID!, base = `/repos/${FROZEN_CUSTODY.repository}`;
  if ((await get1(get, base + "/git/ref/heads/main")).object?.sha !== source) no("stale", "main_moved");
  const run = await get1(get, base + "/actions/runs/" + runId);
  if (String(run.id) !== runId || !fromThisWorkflow(run)) no("unqualified", "run");
  if (run.head_sha !== source) no("mismatch", "run_sha");
  if (!titled(run, mode, source)) no("mismatch", "run_mode");
  const gate = await as("unqualified", "environment", () =>
    requireProtectedEnvironment(get, base, FROZEN_CUSTODY.environment, true, { environment: "environment", branch: "environment_branch" }));
  await as("unqualified", "ci", () =>
    // The full default push proof of the schema-release gate (no override): ci.yml + pg-proof.yml, all named jobs.
    requireGreenPushRuns(get, base, source, { runs: "ci", jobs: "ci_jobs" }));
  let planRun: string | null = null;
  if (mode !== "plan") {
    planRun = env.CUSTODY_PLAN_RUN!;
    const plan = await get1(get, base + "/actions/runs/" + planRun);
    if (String(plan.id) !== planRun || planRun === runId || !fromThisWorkflow(plan) || !titled(plan, "plan", plan.head_sha) ||
      plan.status !== "completed" || plan.conclusion !== "success")
      no("unqualified", "plan_run");
    if (plan.head_sha !== source) no("mismatch", "plan_sha");
    const created = Date.parse(plan.created_at);
    if (!Number.isFinite(created) || created > Date.now() || Date.now() - created > FROZEN_CUSTODY.planMaxAgeMs) no("stale", "plan_age");
  }
  if (o.protectedJob) {
    await as("unqualified", "human_approval", () =>
      requireHumanApproval(get, base, runId, FROZEN_CUSTODY.environment, gate, true, { approval: "human_approval", rejected: "human_rejected", dispatch: "human_dispatch" }));
    // This run is deployed to the environment (the job really runs under it). Field semantics: see the doc.
    const deployments = await get1(get, `${base}/deployments?environment=${FROZEN_CUSTODY.environment}&sha=${source}&per_page=100`);
    const link = new RegExp(`^https://github\\.com/${FROZEN_CUSTODY.repository}/actions/runs/${runId}(/|$)`);
    let linked = false;
    for (const d of Array.isArray(deployments) ? deployments : []) {
      if (d?.environment !== FROZEN_CUSTODY.environment || d?.sha !== source || !Number.isSafeInteger(d?.id)) continue;
      const statuses = await get1(get, `${base}/deployments/${d.id}/statuses?per_page=100`);
      if (Array.isArray(statuses) && statuses.some((s: Obj) => s?.state === "in_progress" && [s.log_url, s.target_url].some((u) => typeof u === "string" && link.test(u)))) {
        linked = true;
        break;
      }
    }
    if (!linked) no("unqualified", "environment_deployment");
  }
  return { planRun };
}

/** The full attestation inside the protected job, after the operator derived `facts` from the live source/target. */
export async function verifySupervisor(env: Env, get: Get, facts: SupervisorFacts): Promise<SupervisorProof> {
  if (env.CUSTODY_MODE !== facts.action) no("mismatch", "mode");
  const ctx = supervisorContextRefusal(env, facts.action);
  if (ctx) throw ctx;
  const approval = env.CUSTODY_APPROVAL!;
  // Seal: the MANIFEST field is taken from the approval (64 hex) and enforced against the in-fence manifest.
  let manifest: string | null = null;
  if (facts.action === "seal") {
    const m = /(?:^| )MANIFEST ([0-9a-f]{64})(?: |$)/.exec(approval);
    if (!m) no("mismatch", "approval_manifest");
    manifest = m![1]!;
  } else if (facts.manifest !== null) no("mismatch", "approval_manifest");
  const want = approvalText({ ...facts, manifest, sha: env.CUSTODY_SHA!, planRun: env.CUSTODY_PLAN_RUN! });
  const diff = approvalDiff(approval, want);
  if (diff) no("mismatch", diff);
  const { planRun } = await verifyCustodyGithub(env, get, { protectedJob: true });
  const proof: SupervisorProof = Object.freeze({
    [BRAND]: true as const,
    action: facts.action,
    manifest,
    epoch: facts.epoch,
    privacy: facts.privacy,
    block: Object.freeze({
      repository_id: FROZEN_CUSTODY.repositoryId,
      workflow: FROZEN_CUSTODY.workflow,
      environment: FROZEN_CUSTODY.environment,
      run_id: env.GITHUB_RUN_ID!,
      run_attempt: 1 as const,
      source_sha: env.CUSTODY_SHA!,
      actor: FROZEN_CUSTODY.dispatcher.login,
      triggering_actor: FROZEN_CUSTODY.dispatcher.login,
      plan_run: planRun!,
      approval_sha256: sha(approval),
      action: facts.action,
      ci_conclusion: "success" as const,
    }),
  });
  minted.add(proof);
  return proof;
}
