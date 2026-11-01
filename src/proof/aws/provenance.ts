/**
 * Provenance: what exactly ran, against what exactly.
 *
 * - The Stage A create-only plan (#75: docs/plans/aws-proof.provenance.json,
 *   aws-proof.plan.txt, aws-proof.inputs.sanitized.json) is loaded and every
 *   digest it records is re-verified over the files. The live cluster must
 *   match it (stage, account, region, cluster ARN, created after the plan was
 *   captured, and running the plan's image digests); otherwise the harness
 *   refuses to run live.
 * - Every report records the harness commit + dirty bit, the config/input
 *   digest, the stage plan digest, and the chart package digests from the
 *   chart repository's index. A PASS needs all of them, from a clean tree.
 */
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { TARGET_VALUES } from "./fence.ts";
import { PROOF_STAGE, SESSIONS_DEV_PROOF_TARGET, type ProofTarget } from "./guard.ts";
import type { DescribedCluster } from "./cluster-binding.ts";

const sha256 = (b: string | Uint8Array) => createHash("sha256").update(b).digest("hex");
const DIGEST = /^sha256:[0-9a-f]{64}$/;

export const STAGE_PLAN_FILES = {
  provenance: "docs/plans/aws-proof.provenance.json",
  plan: "docs/plans/aws-proof.plan.txt",
  inputs: "docs/plans/aws-proof.inputs.sanitized.json",
} as const;

/** The captured plan files of a hardcoded target's stage (aws-proof: STAGE_PLAN_FILES). */
export const stagePlanFiles = (stage: string) =>
  stage === PROOF_STAGE
    ? STAGE_PLAN_FILES
    : ({ provenance: `docs/plans/${stage}.provenance.json`, plan: `docs/plans/${stage}.plan.txt`, inputs: `docs/plans/${stage}.inputs.sanitized.json` } as const);

/** Operations a plan may contain for the harness to accept it: nothing existing is changed. */
export const ACCEPTED_PLAN_OPS = ["create", "noop"] as const;

/**
 * Resources a reviewed stage change may update (the two-tenant change in #75
 * updates the umbrella release and the AlleyOop role). Naming them is never
 * enough: updates are admitted only with an approval bound to the exact
 * capture (PlanUpdateApproval). Replace, delete, import and adopt are refused
 * unconditionally.
 */
export const REVIEWED_PLAN_UPDATES = ["96bulls", "alleyoop-role"] as const;

/** The reviewed capture an update approval is bound to; every digest must match the loaded plan exactly. */
export interface PlanUpdateApproval {
  provenanceSha256: string;
  planSha256: string;
  inputsSha256: string;
  sourceCommit: string;
}

const APPROVAL = /^([0-9a-f]{64}):([0-9a-f]{64}):([0-9a-f]{64}):([0-9a-f]{40})$/;

/** `<provenanceSha256>:<planSha256>:<inputsSha256>:<sourceCommit>`; anything else (a resource name included) is refused. */
export function parsePlanUpdateApproval(v: string): PlanUpdateApproval | { error: string } {
  const m = APPROVAL.exec(v);
  if (!m) return { error: "--allow-plan-update must be <provenanceSha256>:<planSha256>:<inputsSha256>:<sourceCommit> (hex digests and a 40-hex commit)" };
  return { provenanceSha256: m[1]!, planSha256: m[2]!, inputsSha256: m[3]!, sourceCommit: m[4]! };
}

export interface PlanOps {
  summary: Record<string, number>;
  resources: Array<{ id: string; op: string }>;
}

const planHeader = (stage: string) => [new RegExp(`^Plan · ${stage.replace(/[^a-z0-9-]/g, "")}$`), /^Importing stack module$/, /^Resolving stack services$/, /^AWS: using environment variables \([A-Z_, ]+\) instead of the profile\.$/, /^Loading stack state$/, /^Computing plan$/, /^Plan ready \([0-9.]+m?s\)$/];

/**
 * Parse the captured plan text: the `Plan: N to create, M to replace` summary
 * and every `[resource] op` line. Unknown lines are refused (fail closed).
 */
export function parsePlanOps(text: string, stage: string = PROOF_STAGE): PlanOps | { error: string } {
  const PLAN_HEADER = planHeader(stage);
  const summary: Record<string, number> = {};
  const resources: PlanOps["resources"] = [];
  let summaries = 0;
  for (const raw of text.split("\n")) {
    const line = raw.trimEnd();
    if (!line) continue;
    const r = /^\[([^\]]+)\] ([a-z-]+)$/.exec(line);
    if (r) {
      resources.push({ id: r[1]!, op: r[2]! });
      continue;
    }
    if (/^Plan: /.test(line)) {
      summaries += 1;
      for (const part of line.slice("Plan: ".length).split(",")) {
        const m = /^\s*(\d+) to ([a-z-]+)\s*$/.exec(part);
        if (!m) return { error: `unrecognized plan summary: ${line}` };
        summary[m[2]!] = Number(m[1]);
      }
      continue;
    }
    if (/^No changes/.test(line)) {
      summaries += 1;
      continue;
    }
    if (!PLAN_HEADER.some((h) => h.test(line))) return { error: `unrecognized plan line: ${line.slice(0, 80)}` };
  }
  if (summaries !== 1) return { error: "plan must have exactly one summary line" };
  return { summary, resources };
}

/**
 * Only create and noop are accepted, in the summary and on every resource line,
 * and they must agree. `updatesApproved` (true only when an approval matched
 * the exact capture) admits `update` for REVIEWED_PLAN_UPDATES and nothing else.
 */
export function planOpsIssues(ops: PlanOps, updatesApproved = false): string[] {
  const issues: string[] = [];
  const okOp = (r: { id: string; op: string }) => (ACCEPTED_PLAN_OPS as readonly string[]).includes(r.op) || (r.op === "update" && updatesApproved && (REVIEWED_PLAN_UPDATES as readonly string[]).includes(r.id));
  for (const [op, n] of Object.entries(ops.summary)) if (n > 0 && !(ACCEPTED_PLAN_OPS as readonly string[]).includes(op) && op !== "update") issues.push(`plan would ${op} ${n} resource(s)`);
  const updates = ops.resources.filter((r) => r.op === "update").length;
  if ((ops.summary.update ?? 0) !== updates) issues.push(`plan summary says ${ops.summary.update ?? 0} to update but lists ${updates}`);
  const bad = ops.resources.filter((r) => !okOp(r));
  for (const r of bad) issues.push(`plan would ${r.op} [${r.id}]`);
  const creates = ops.resources.filter((r) => r.op === "create").length;
  if ((ops.summary.create ?? 0) !== creates) issues.push(`plan summary says ${ops.summary.create ?? 0} to create but lists ${creates}`);
  if (new Set(ops.resources.map((r) => r.id)).size !== ops.resources.length) issues.push("plan lists a resource twice");
  if (!ops.resources.some((r) => r.id === "cluster")) issues.push("plan has no [cluster] resource");
  return issues;
}

export interface StagePlan {
  provenanceSha256: string;
  generatedAt: string;
  commit: string;
  planSha256: string;
  inputsSha256: string;
  servicesDigest: string;
  relaycastDigest: string;
  chartVersions: { "96bulls": string; relayauth: string; relaycast: string };
  /** Reviewed updates the plan carries (admitted by a matching approval); empty for a create-only plan. */
  allowedUpdates: string[];
  /** The digest-bound approval that admitted them, or null. */
  updateApproval: PlanUpdateApproval | null;
  /** create: this plan creates the cluster; noop: it already existed when the plan was captured. */
  clusterOp: "create" | "noop";
  sessionDirectoryEnabled: boolean;
}

/** Load and re-verify the captured Stage A plan. Every mismatch is a refusal. */
export function loadStagePlan(root: string, read: (p: string) => string | undefined = (p) => (existsSync(p) ? readFileSync(p, "utf8") : undefined), approval?: PlanUpdateApproval, t: ProofTarget = SESSIONS_DEV_PROOF_TARGET): StagePlan | { error: string } {
  const tv = TARGET_VALUES[t.id];
  if (tv.target !== t) return { error: "not a hardcoded proof target" };
  const STAGE_PLAN_FILES = stagePlanFiles(t.stage);
  const raw = read(join(root, STAGE_PLAN_FILES.provenance));
  if (raw === undefined) return { error: `stage plan provenance ${STAGE_PLAN_FILES.provenance} is missing (capture it with \`bin/96bulls plan ${t.stage} --capture\`${t.stage === PROOF_STAGE ? ", #75" : ""})` };
  let p: {
    stage?: string; generatedAt?: string; target?: { accountId?: string; region?: string };
    source?: { commit?: string; clean?: boolean }; inputs?: { sanitizedPath?: string; sanitizedSha256?: string };
    outputs?: { plan?: { path?: string; sha256?: string } };
  };
  try {
    p = JSON.parse(raw);
  } catch {
    return { error: "stage plan provenance is not JSON" };
  }
  if (p.stage !== t.stage) return { error: `stage plan is for ${p.stage}, not ${t.stage}` };
  if (p.target?.accountId !== t.accountId || p.target?.region !== t.region) return { error: "stage plan targets another account or region" };
  if (p.source?.clean !== true || !/^[0-9a-f]{40}$/.test(p.source?.commit ?? "")) return { error: "stage plan was not captured from a clean commit" };
  if (Number.isNaN(Date.parse(p.generatedAt ?? ""))) return { error: "stage plan generatedAt invalid" };
  if (p.outputs?.plan?.path !== STAGE_PLAN_FILES.plan || p.inputs?.sanitizedPath !== STAGE_PLAN_FILES.inputs) return { error: "stage plan points at unexpected files" };
  const plan = read(join(root, STAGE_PLAN_FILES.plan));
  if (plan === undefined || sha256(plan) !== p.outputs.plan.sha256) return { error: "stage plan text does not match its recorded sha256" };
  const inputsRawEarly = read(join(root, STAGE_PLAN_FILES.inputs));
  if (inputsRawEarly === undefined || sha256(inputsRawEarly) !== p.inputs.sanitizedSha256) return { error: "stage plan inputs do not match their recorded sha256" };
  // An update approval must name this exact capture: provenance bytes, plan bytes, input bytes and source commit.
  if (approval) {
    const actual: PlanUpdateApproval = { provenanceSha256: sha256(raw), planSha256: sha256(plan), inputsSha256: sha256(inputsRawEarly), sourceCommit: p.source.commit! };
    for (const k of ["provenanceSha256", "planSha256", "inputsSha256", "sourceCommit"] as const)
      if (approval[k] !== actual[k]) return { error: `plan update approval does not match this capture (${k})` };
  }
  const ops = parsePlanOps(plan, t.stage);
  if ("error" in ops) return { error: `stage plan unreadable: ${ops.error}` };
  const opIssues = planOpsIssues(ops, !!approval);
  if (opIssues.length) return { error: `stage plan is not create-only: ${opIssues.join("; ")}` };
  if ((p as { summary?: string }).summary !== undefined && !plan.split("\n").some((l) => l.trim() === (p as { summary?: string }).summary)) return { error: "stage plan provenance summary does not match the plan text" };
  const clusterOp = ops.resources.find((r) => r.id === "cluster")!.op as "create" | "noop";
  const inputsRaw = inputsRawEarly;
  const i = JSON.parse(inputsRaw) as {
    accountId?: string; region?: string;
    accountShared?: unknown; pippenSandbox?: { permissionsBoundaryArn?: string };
    eks?: { chartVersion?: string; imageDigest?: string; relayhistoryOrigin?: string; ingress?: { hostname?: string }; relayauth?: { chartVersion?: string }; relaycast?: { chartVersion?: string; imageDigest?: string }; sessionDirectory?: { enabled?: boolean } };
  };
  if (i.accountId !== t.accountId || i.region !== t.region) return { error: "stage plan inputs target another account or region" };
  if (i.eks?.ingress?.hostname !== tv.host || i.eks?.relayhistoryOrigin !== tv.origin) return { error: "stage plan inputs name another host or origin" };
  // The Pippen sandbox is a fresh account: it must be planned as one (boundary on every role, sweeper tag), never as a shared account.
  if (t.id === "pippen-sandbox") {
    if (i.accountShared !== undefined) return { error: "stage plan inputs share another stage's account resources; a Pippen sandbox must not set accountShared" };
    if (!new RegExp(`^arn:aws:iam::${t.accountId}:policy/[\\w+=,.@/-]+$`).test(i.pippenSandbox?.permissionsBoundaryArn ?? ""))
      return { error: `stage plan inputs lack pippenSandbox.permissionsBoundaryArn in ${t.accountId}` };
  }
  if (!DIGEST.test(i.eks?.imageDigest ?? "") || !DIGEST.test(i.eks?.relaycast?.imageDigest ?? "")) return { error: "stage plan inputs lack pinned image digests" };
  const v = (x: string | undefined) => (x && /^\d+\.\d+\.\d+$/.test(x) ? x : undefined);
  const charts = { "96bulls": v(i.eks?.chartVersion), relayauth: v(i.eks?.relayauth?.chartVersion), relaycast: v(i.eks?.relaycast?.chartVersion) };
  if (!charts["96bulls"] || !charts.relayauth || !charts.relaycast) return { error: "stage plan inputs lack chart versions" };
  return {
    provenanceSha256: sha256(raw),
    generatedAt: p.generatedAt!,
    commit: p.source!.commit!,
    planSha256: p.outputs.plan.sha256!,
    inputsSha256: p.inputs.sanitizedSha256!,
    servicesDigest: i.eks!.imageDigest!,
    relaycastDigest: i.eks!.relaycast!.imageDigest!,
    chartVersions: charts as StagePlan["chartVersions"],
    clusterOp,
    allowedUpdates: ops.resources.filter((r) => r.op === "update").map((r) => r.id),
    updateApproval: approval ?? null,
    sessionDirectoryEnabled: i.eks?.sessionDirectory?.enabled === true,
  };
}

/**
 * The live cluster must agree with the plan: same ARN (checked by describe);
 * created after the capture when the plan creates it, or already existing at
 * capture when the plan leaves it unchanged (a continuation plan).
 */
export function bindStagePlan(plan: StagePlan, cluster: DescribedCluster): { ok: true } | { error: string } {
  const at = Date.parse(plan.generatedAt);
  if (plan.clusterOp === "create" && cluster.createdAtMs < at) return { error: "the live cluster predates the captured plan; it was not created by it" };
  if (plan.clusterOp === "noop" && cluster.createdAtMs > at) return { error: "the plan found the cluster already present, but the live cluster was created after the capture" };
  return { ok: true };
}

/** Running images must include the plan's services and Relaycast digests (fresh-install compares every image). */
export function planImageIssues(plan: StagePlan, runningDigests: readonly string[]): string[] {
  const issues: string[] = [];
  if (!runningDigests.includes(plan.servicesDigest)) issues.push(`no running container uses the plan's services digest ${plan.servicesDigest}`);
  if (!runningDigests.includes(plan.relaycastDigest)) issues.push(`no running container uses the plan's Relaycast digest ${plan.relaycastDigest}`);
  return issues;
}

/* ------------------------------------------------------------- report provenance */

export interface ReportProvenance {
  harness: { commit: string; dirty: boolean };
  /** sha256 of inputs.json bytes, or of the defaults when there is no file. */
  inputs: { sha256: string; source: "file" | "defaults" };
  /** sha256 of the effective config the run used. */
  configSha256: string;
  stagePlan: { provenanceSha256: string; generatedAt: string; commit: string; planSha256: string; allowedUpdates: string[]; updateApproval: PlanUpdateApproval | null } | null;
  charts: Array<{ name: string; version: string; digest: string | null }>;
}

export function gitProvenance(run: (argv: string[]) => { stdout: string; status: number | null }): ReportProvenance["harness"] {
  const head = run(["git", "rev-parse", "HEAD"]);
  const st = run(["git", "status", "--porcelain", "--untracked-files=normal"]);
  const commit = head.status === 0 && /^[0-9a-f]{40}$/.test(head.stdout.trim()) ? head.stdout.trim() : "unknown";
  return { commit, dirty: st.status !== 0 || st.stdout.trim() !== "" };
}

/** Chart package digests from a chart-releaser index.yaml. */
export function chartDigests(indexYaml: string, wanted: ReadonlyArray<{ name: string; version: string }>): ReportProvenance["charts"] {
  const found = new Map<string, string>();
  let chart = "";
  let item: { digest?: string; version?: string } = {};
  const flush = () => {
    if (chart && item.version && item.digest) found.set(`${chart}@${item.version}`, item.digest);
    item = {};
  };
  for (const line of indexYaml.split("\n")) {
    const c = /^ {2}([A-Za-z0-9._-]+):\s*$/.exec(line);
    if (c) {
      flush();
      chart = c[1]!;
      continue;
    }
    if (/^ {2}- /.test(line)) flush();
    const d = /^ {2}(?:- | {2})digest: ([0-9a-f]{64})\s*$/.exec(line);
    if (d) item.digest = d[1];
    const v = /^ {2}(?:- | {2})version: "?([0-9A-Za-z.+-]+)"?\s*$/.exec(line);
    if (v) item.version = v[1];
    if (/^\S/.test(line)) {
      flush();
      chart = "";
    }
  }
  flush();
  return wanted.map((w) => ({ name: w.name, version: w.version, digest: found.get(`${w.name}@${w.version}`) ?? null }));
}

/** What a PASS additionally requires of provenance. Empty = complete. */
export function provenanceGaps(p: ReportProvenance | undefined): string[] {
  if (!p) return ["provenance missing"];
  const gaps: string[] = [];
  if (!/^[0-9a-f]{40}$/.test(p.harness.commit)) gaps.push("harness commit unknown");
  if (p.harness.dirty) gaps.push("harness working tree is dirty");
  if (!p.stagePlan) gaps.push("no stage plan bound");
  if (!p.charts.length || p.charts.some((c) => !c.digest)) gaps.push("chart package digests missing");
  return gaps;
}

export const sha256Hex = sha256;
