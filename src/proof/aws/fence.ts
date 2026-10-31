/**
 * The target fence. The guard (guard.ts) proves *who* we are and *which
 * cluster* we talk to; the fence proves *what* each step touches: every bucket,
 * namespace, release, deployment, context, region, ARN, endpoint and hostname
 * must be the proof stage's own. It runs twice:
 *
 *   - on the config, before anything is planned (proofConfigIssues);
 *   - on every step's argv, in every mode including dry-run (stepFenceIssue),
 *     so a resource that slipped past config validation still cannot run.
 *
 * Production-shaped names are refused everywhere, even inside the proof account.
 */
import { PROOF_ACCOUNT_ID, PROOF_CLUSTER, PROOF_STAGE } from "./guard.ts";

/** The proof stage's public ingest host and RelayHistory origin. Never sessions-dev or a production host. */
export const PROOF_HOST = "proof-sessions.agentrelay.com";
export const PROOF_ORIGIN = `https://${PROOF_HOST}`;
/** Every AWS resource name the stack creates for this stage starts with this (stacks/aws/alchemy.aws.run.ts `name()`). */
export const PROOF_NAME_PREFIX = `96bulls-${PROOF_STAGE}-`;
export const PROOF_LOCKER_BUCKET = `${PROOF_NAME_PREFIX}locker-${PROOF_ACCOUNT_ID}`;
export const PROOF_RELEASE = "96bulls";
export const PROOF_CHART_REPO = "https://AgentWorkforce.github.io/helm-charts";
/**
 * In-cluster namespaces are fixed by stacks/aws/eks.ts and relayauth.ts; inside
 * the fenced cluster `96bulls-aws-proof` they can only be the proof stage's.
 */
export const PROOF_NAMESPACES = ["96bulls", "relayauth", "relaycast", "cnpg-system"] as const;

/** Production- or shared-shaped tokens. Matched as whole segments, case-insensitive. */
export const FORBIDDEN_NAME = /(^|[^a-z0-9])(prod|production|sessions-prod|sessions-dev|staging|hosted)([^a-z0-9]|$)/i;
/** Any URL carrying userinfo (`scheme://user[:pass]@host`). */
export const URL_USERINFO = /\b[a-z][a-z0-9+.-]*:\/\/[^\s/?#@]+@/i;

export interface Fence {
  context: string;
  region: string;
  namespaces: ReadonlySet<string>;
  buckets: ReadonlySet<string>;
  deployments: ReadonlySet<string>;
  hosts: ReadonlySet<string>;
  /** Account every ARN must carry; undefined only for the local kind fence (no ARNs allowed). */
  accountId?: string;
}

interface FenceSource {
  stage: string;
  kubeContext: string;
  region: string;
  namespace: string;
  release: string;
  chartRepo: string;
  coreDeployments: string[];
  hoopdb: { cluster: string; database: string };
  locker: { bucket: string };
  backupBucket?: string;
  relayauth: { namespace: string; service: string };
  relaycast: { namespace: string; service: string };
  ingest: { service: string };
  ingress: { hostname: string };
  historyOrigin: string;
  transcriptSearch: { backend: string; endpoint?: string; database?: string };
}

const hostOf = (u: string | undefined) => {
  try {
    return u ? new URL(u).hostname : undefined;
  } catch {
    return undefined;
  }
};

export function fenceFor(c: FenceSource): Fence {
  const t = hostOf(c.transcriptSearch.endpoint);
  return {
    context: c.kubeContext,
    region: c.region,
    namespaces: new Set([c.namespace, c.relayauth.namespace, c.relaycast.namespace, "cnpg-system"]),
    buckets: new Set([c.locker.bucket, ...(c.backupBucket ? [c.backupBucket] : [])]),
    deployments: new Set([...c.coreDeployments, c.relayauth.service, c.relaycast.service]),
    // The chart repository is read (helm show values) and pinned to PROOF_CHART_REPO by proofConfigIssues.
    hosts: new Set(["127.0.0.1", c.ingress.hostname, hostOf(c.chartRepo), ...(t ? [t] : [])].filter((h): h is string => !!h).map((h) => h.toLowerCase())),
    ...(c.stage === PROOF_STAGE ? { accountId: PROOF_ACCOUNT_ID } : {}),
  };
}

/** Every string anywhere in a value, with its path. */
function strings(v: unknown, path = ""): Array<[string, string]> {
  if (typeof v === "string") return [[path, v]];
  if (Array.isArray(v)) return v.flatMap((x, i) => strings(x, `${path}[${i}]`));
  if (v && typeof v === "object") return Object.entries(v).flatMap(([k, x]) => strings(x, path ? `${path}.${k}` : k));
  return [];
}

/** Config-level fence for the proof stage. Empty array = every target is the proof stage's. */
export function proofConfigIssues(c: FenceSource & Record<string, unknown>): string[] {
  const issues: string[] = [];
  for (const [path, v] of strings(c)) {
    if (FORBIDDEN_NAME.test(v)) issues.push(`${path} is production- or shared-shaped`);
    if (URL_USERINFO.test(v)) issues.push(`${path} carries URL credentials (userinfo); credentials come from the environment only`);
  }
  if (c.stage !== PROOF_STAGE) return issues;
  const want = (path: string, got: string, exp: string) => got !== exp && issues.push(`${path} must be ${exp} (got ${got})`);
  want("ingress.hostname", c.ingress.hostname, PROOF_HOST);
  want("historyOrigin", c.historyOrigin, PROOF_ORIGIN);
  want("release", c.release, PROOF_RELEASE);
  want("chartRepo", c.chartRepo, PROOF_CHART_REPO);
  want("locker.bucket", c.locker.bucket, PROOF_LOCKER_BUCKET);
  want("hoopdb.cluster", c.hoopdb.cluster, "hoopdb");
  if (!/^arn:aws:eks:[a-z0-9-]+:\d{12}:cluster\/.+$/.test(c.kubeContext) || !c.kubeContext.endsWith(`:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER}`))
    issues.push(`kubeContext must be the proof cluster's ARN (…:${PROOF_ACCOUNT_ID}:cluster/${PROOF_CLUSTER})`);
  if (c.backupBucket !== undefined && !(c.backupBucket.startsWith(PROOF_NAME_PREFIX) && c.backupBucket.endsWith(`-${PROOF_ACCOUNT_ID}`)))
    issues.push(`backupBucket must be ${PROOF_NAME_PREFIX}<name>-${PROOF_ACCOUNT_ID}`);
  for (const [path, ns] of [["namespace", c.namespace], ["relayauth.namespace", c.relayauth.namespace], ["relaycast.namespace", c.relaycast.namespace]] as const)
    if (!(PROOF_NAMESPACES as readonly string[]).includes(ns)) issues.push(`${path} must be one of ${PROOF_NAMESPACES.join(", ")}`);
  for (const d of [...c.coreDeployments, c.ingest.service]) if (!d.startsWith(`${PROOF_RELEASE}-`)) issues.push(`deployment/service ${d} must belong to release ${PROOF_RELEASE}`);
  if (c.transcriptSearch.backend !== "none" && !(c.transcriptSearch.database ?? "").startsWith("proof"))
    issues.push("transcriptSearch.database must be a proof database (name starting with 'proof')");
  return issues;
}

const valueAfter = (argv: readonly string[], flags: readonly string[]) => {
  const out: string[] = [];
  argv.forEach((a, i) => {
    for (const f of flags) {
      if (a === f && argv[i + 1] !== undefined) out.push(argv[i + 1]!);
      else if (a.startsWith(`${f}=`)) out.push(a.slice(f.length + 1));
    }
  });
  return out;
};

const placeholder = (v: string) => v.startsWith("<");

/**
 * Runtime fence on one step. Returns the first reason the step's argv names a
 * target outside the fence, or undefined. Placeholders (`<...>`) appear only in
 * dry-run plans and are allowed.
 */
export function stepFenceIssue(argv: readonly string[], f: Fence): string | undefined {
  for (const a of argv) {
    if (FORBIDDEN_NAME.test(a)) return "production_shaped_target";
    if (URL_USERINFO.test(a)) return "url_credentials";
  }
  for (const v of valueAfter(argv, ["--context", "--kube-context"])) if (v !== f.context) return "context_outside_fence";
  for (const v of valueAfter(argv, ["--region"])) if (v !== f.region) return "region_outside_fence";
  for (const v of valueAfter(argv, ["-n", "--namespace"])) if (!f.namespaces.has(v)) return "namespace_outside_fence";
  for (const v of valueAfter(argv, ["--bucket"])) if (!f.buckets.has(v)) return "bucket_outside_fence";
  for (const a of argv) {
    for (const m of a.matchAll(/s3:\/\/([^/\s'"]+)/g)) if (!f.buckets.has(m[1]!)) return "bucket_outside_fence";
    for (const m of a.matchAll(/(?:^|\s)deployment\/([A-Za-z0-9._-]+)/g)) if (!f.deployments.has(m[1]!)) return "deployment_outside_fence";
    for (const m of a.matchAll(/\bhttps?:\/\/([^/:\s'"?#]+)/g)) {
      const host = m[1]!.toLowerCase();
      // In-cluster service DNS is allowed only inside a fenced namespace: <svc>.<ns>.svc
      const svc = /^[a-z0-9-]+\.([a-z0-9-]+)\.svc(\.cluster\.local)?$/.exec(host);
      if (svc ? !f.namespaces.has(svc[1]!) : !f.hosts.has(host) && !placeholder(host)) return "host_outside_fence";
    }
    for (const m of a.matchAll(/arn:aws:[a-z0-9-]+:[a-z0-9-]*:(\d{12})?:/g)) if (m[1] && m[1] !== f.accountId) return "arn_outside_fence";
  }
  return undefined;
}
